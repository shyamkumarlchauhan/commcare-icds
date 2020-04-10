from django.db import transaction
from django.utils.functional import cached_property

from corehq.apps.locations.models import LocationType, SQLLocation
from custom.icds.location_reassignment.const import SPLIT_OPERATION
from custom.icds.location_reassignment.exceptions import (
    InvalidTransitionError,
    LocationCreateError,
)
from custom.icds.location_reassignment.utils import deprecate_locations


class Processor(object):
    def __init__(self, domain, transitions, new_location_details, site_codes):
        """
        :param domain: domain
        :param transitions: transitions in format generated by Parser
        :param new_location_details: details necessary to create new locations
        :param site_codes: site codes of all locations undergoing transitions
        """
        self.domain = domain
        self.location_types_by_code = {lt.code: lt for lt in LocationType.objects.by_domain(self.domain)}
        self.transitions = transitions
        self.new_location_details = new_location_details
        self.site_codes = site_codes

    def process(self):
        self._create_new_locations()
        # process each sheet, in reverse order of hierarchy
        for location_type_code in list(reversed(list(self.location_types_by_code))):
            for operation, transitions in self.transitions[location_type_code].items():
                self._perform_transitions(operation, transitions)

    def _create_new_locations(self):
        locations_by_site_code = self._get_existing_parent_locations()

        with transaction.atomic():
            for location_type_code in self.location_types_by_code:
                new_locations_details = self.new_location_details.get(location_type_code, [])
                for details in new_locations_details:
                    parent_location = None
                    if details['parent_site_code']:
                        parent_location = locations_by_site_code[details['parent_site_code']]
                    location = SQLLocation.objects.create(
                        domain=self.domain, site_code=details['site_code'], name=details['name'],
                        parent=parent_location,
                        location_type=self.location_types_by_code[location_type_code],
                        metadata={'lgd_code': details['lgd_code']}
                    )
                    # add new location in case its a parent to any other locations getting created
                    locations_by_site_code[details['site_code']] = location

    def _get_existing_parent_locations(self):
        existing_parent_site_codes = set()

        for location_type_code in list(reversed(list(self.location_types_by_code))):
            new_locations_details = self.new_location_details.get(location_type_code)
            if new_locations_details:
                for details in new_locations_details:
                    if details['parent_site_code']:
                        existing_parent_site_codes.add(details['parent_site_code'])
                    # remove it from parent site codes if it itself needs to be created
                    existing_parent_site_codes.discard(details['site_code'])

        if existing_parent_site_codes:
            return self._get_locations_by_site_codes(existing_parent_site_codes)
        return {}

    def _get_locations_by_site_codes(self, site_codes):
        parents = SQLLocation.active_objects.filter(domain=self.domain, site_code__in=site_codes)
        if len(parents) != len(site_codes):
            missing_site_codes = site_codes - set([loc.site_code for loc in parents])
            raise LocationCreateError(
                "Could not find parent locations with following site codes %s" % ",".join(missing_site_codes))
        return {
            loc.site_code: loc for loc in parents
        }

    def _perform_transitions(self, operation, transitions):
        # split operation has the old site code as the key whereas others have the new site code
        for from_site_codes, to_site_codes in transitions.items():
            if operation == SPLIT_OPERATION:
                old_site_codes, new_site_codes = from_site_codes, to_site_codes
            else:
                new_site_codes, old_site_codes = from_site_codes, to_site_codes
            errors = deprecate_locations(self._get_locations(old_site_codes), self._get_locations(new_site_codes),
                                         operation)
            if errors:
                raise InvalidTransitionError(",".join(errors))

    def _get_locations(self, site_codes):
        site_codes = site_codes if isinstance(site_codes, list) else [site_codes]
        locations = [self.locations_by_site_code.get(site_code)
                     for site_code in site_codes
                     if self.locations_by_site_code.get(site_code)]
        if len(locations) != len(site_codes):
            loaded_site_codes = [loc.site_code for loc in locations]
            missing_site_codes = set(site_codes) - set(loaded_site_codes)
            raise InvalidTransitionError(
                "Could not load location with following site codes: %s" % ",".join(missing_site_codes))
        return locations

    @cached_property
    def locations_by_site_code(self):
        return {
            loc.site_code: loc
            for loc in SQLLocation.active_objects.filter(domain=self.domain, site_code__in=self.site_codes)
        }
