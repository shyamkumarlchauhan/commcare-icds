from collections import defaultdict

import attr

from corehq.apps.locations.models import LocationType, SQLLocation
from corehq.apps.users.models import CommCareUser
from corehq.apps.users.util import normalize_username
from custom.icds.location_reassignment.const import (
    AWC_CODE_COLUMN,
    CURRENT_SITE_CODE_COLUMN,
    EXTRACT_OPERATION,
    HOUSEHOLD_ID_COLUMN,
    MERGE_OPERATION,
    NEW_LGD_CODE,
    NEW_NAME,
    NEW_PARENT_SITE_CODE,
    NEW_SITE_CODE_COLUMN,
    NEW_SUB_DISTRICT_NAME,
    NEW_USERNAME_COLUMN,
    OPERATION_COLUMN,
    SPLIT_OPERATION,
    USERNAME_COLUMN,
    VALID_OPERATIONS,
)
from custom.icds.location_reassignment.models import Transition


class TransitionRow(object):
    """
    An object representation of each row in excel
    """
    def __init__(self, location_type, operation, old_site_code, new_site_code, expects_parent,
                 new_location_details=None, old_username=None, new_username=None):
        self.location_type = location_type
        self.operation = operation
        self.old_site_code = old_site_code
        self.new_site_code = new_site_code
        self.expects_parent = expects_parent
        self.new_location_details = new_location_details or {}
        self.old_username = old_username
        self.new_username = new_username

    def validate(self):
        """
        a. operation column has a valid value
            b. If its a valid operation
                i. there should be both old and new location codes
                ii. there should be a change in old and new location codes
                iii. there should be both old and new usernames or neither
                iv. there should be a new parent site code where expected
                v. there should be no new parent site code where not expected
        """
        if self.operation not in VALID_OPERATIONS:
            return [f"Invalid Operation {self.operation}"]

        if not self.old_site_code or not self.new_site_code:
            return [f"Missing location code for operation {self.operation}. "
                    f"Got old: '{self.old_site_code}' and new: '{self.new_site_code}'"]

        errors = []
        if self.old_site_code == self.new_site_code:
            errors.append(f"No change in location code for operation {self.operation}. "
                          f"Got old: '{self.old_site_code}' and new: '{self.new_site_code}'")

        if bool(self.new_username) != bool(self.old_username):
            errors.append(f"Need both old and new username for {self.operation} operation "
                          f"on location '{self.old_site_code}'")
        if not self.new_location_details.get('name', '').strip():
            errors.append(f"Missing new location name for {self.new_site_code}")
        if self.expects_parent and not self.new_location_details.get('parent_site_code'):
            errors.append(f"Need parent for '{self.new_site_code}'")
        if not self.expects_parent and self.new_location_details.get('parent_site_code'):
            errors.append(f"Unexpected parent set for '{self.new_site_code}'")
        return errors


class Parser(object):
    def __init__(self, domain, workbook):
        """
        Receives a worksheet generated by custom.icds.location_reassignment.download.Download
        and generates an output like
        {
            location_type_code:
                {
                    'location site code': Transition object
                }
        }

        Find valid transitions and then additionally validates that
            a. all old locations should be present in the system
            b. if a location is deprecated, all its descendants should get deprecated too
            c. new parent assigned should be of the expected location type
        """
        self.domain = domain
        self.workbook = workbook

        # For consolidated validations
        # maintain a list of all site codes undergoing a transition
        self.transiting_site_codes = set()
        # maintain a list of valid site codes to be deprecated i.e all old site codes
        self.site_codes_to_be_deprecated = set()
        # maintain a list of valid site codes to be archived
        self.site_codes_to_be_archived = set()

        # mapping of expected parent type for a location type
        self.location_type_parent = {
            lt.code: lt.parent_type.code
            for lt in LocationType.objects.select_related('parent_type').filter(domain=self.domain)
            if lt.parent_type
        }
        location_type_codes_in_hierarchy = [lt.code for lt in LocationType.objects.by_domain(self.domain)]

        # Details of requested changes
        # site codes of new locations getting created
        self.new_site_codes_for_location_type = {
            location_type_code: set()
            for location_type_code in location_type_codes_in_hierarchy
        }
        # a mapping of all TransitionRows passed for each location type
        self.transition_rows = {location_type_code: defaultdict(list)
                                for location_type_code in location_type_codes_in_hierarchy}
        # a list of all normalized usernames passed
        self.usernames = set()
        # a mapping of all valid transitions found
        self.valid_transitions = {location_type_code: []
                                  for location_type_code in location_type_codes_in_hierarchy}
        self.errors = []

    def parse(self):
        for worksheet in self.workbook.worksheets:
            location_type_code = worksheet.title
            expects_parent = bool(self.location_type_parent.get(location_type_code))
            for row in worksheet:
                operation = row.get(OPERATION_COLUMN)
                if not operation:
                    continue
                transition_row = TransitionRow(
                    location_type=location_type_code,
                    operation=operation,
                    old_site_code=row.get(CURRENT_SITE_CODE_COLUMN),
                    new_site_code=row.get(NEW_SITE_CODE_COLUMN),
                    expects_parent=expects_parent,
                    new_location_details={
                        'name': row.get(NEW_NAME),
                        'parent_site_code': row.get(NEW_PARENT_SITE_CODE),
                        'lgd_code': row.get(NEW_LGD_CODE),
                        'sub_district_name': row.get(NEW_SUB_DISTRICT_NAME)
                    },
                    old_username=row.get(USERNAME_COLUMN),
                    new_username=row.get(NEW_USERNAME_COLUMN)
                )
                self._note_transition(transition_row)
        self._consolidate()
        self.validate()
        return self.errors

    def _note_transition(self, row):
        operation = row.operation
        location_type_code = row.location_type
        new_site_code = row.new_site_code
        old_site_code = row.old_site_code
        # only for merge operation final location is used as the reference key
        if operation == MERGE_OPERATION:
            self.transition_rows[location_type_code][new_site_code].append(row)
        else:
            self.transition_rows[location_type_code][old_site_code].append(row)

    def _consolidate(self):
        """
        Consolidate valid TransitionRow requests into valid Transition objects
        In case of multiple TransitionRow requests, like in case of merge/split,
        combine them into one Transition
        """
        for location_type_code, rows_for_site_code in self.transition_rows.items():
            for site_code, rows in rows_for_site_code.items():
                errors = []
                for row in rows:
                    errors.extend(row.validate())
                if errors:
                    self.errors.extend(errors)
                    continue

                operation = self._valid_unique_operation(site_code, rows)
                if not operation:
                    continue

                transition = self._consolidated_transition(location_type_code, operation, rows)
                if not transition:
                    continue
                if self._is_valid_transition(transition):
                    self.site_codes_to_be_deprecated.update(transition.old_site_codes)
                    if transition.operation_obj.archives_old_locations:
                        self.site_codes_to_be_archived.update(transition.old_site_codes)
                    self.valid_transitions[location_type_code].append(transition)
                    for old_username, new_username in transition.user_transitions.items():
                        if old_username:
                            self.usernames.add(normalize_username(old_username, self.domain))
                        if new_username:
                            self.usernames.add(normalize_username(new_username, self.domain))

                # keep note of transition details for consolidated validations
                self.transiting_site_codes.update(transition.old_site_codes)
                self.transiting_site_codes.update(transition.new_site_codes)
                self.new_site_codes_for_location_type[location_type_code].update(transition.new_site_codes)

    def _valid_unique_operation(self, site_code, rows):
        """
        return unique valid operation for rows
        """
        unique_operations = {row.operation for row in rows}
        if len(unique_operations) > 1:
            self.errors.append(f"Different operations requested for {site_code}: {','.join(unique_operations)}")
            return None
        operation = rows[0].operation

        if len(rows) > 1 and operation not in [MERGE_OPERATION, SPLIT_OPERATION]:
            self.errors.append(f"Multiple {operation} rows for {site_code}")
            return None
        if not len(rows) > 1 and operation in [MERGE_OPERATION, SPLIT_OPERATION]:
            self.errors.append(f"Expected multiple rows for {operation} for {site_code}")
            return None
        return operation

    def _consolidated_transition(self, location_type_code, operation, rows):
        transition = Transition(domain=self.domain, location_type_code=location_type_code, operation=operation)
        for row in rows:
            if row.new_site_code in transition.new_location_details:
                # new location is passed with different details
                if transition.new_location_details[row.new_site_code] != row.new_location_details:
                    self.errors.append(f"New location {row.new_site_code} passed with different information")
                    return None
            transition.add(
                old_site_code=row.old_site_code,
                new_site_code=row.new_site_code,
                new_location_details=row.new_location_details,
                old_username=row.old_username,
                new_username=row.new_username
            )
        return transition

    def _is_valid_transition(self, transition):
        valid = True
        for old_site_code in transition.old_site_codes:
            if old_site_code in self.transiting_site_codes:
                self.errors.append(f"{old_site_code} participating in multiple transitions")
                valid = False
        for new_site_code in transition.new_site_codes:
            if new_site_code in self.transiting_site_codes:
                self.errors.append(f"{new_site_code} participating in multiple transitions")
                valid = False
        return valid

    def validate(self):
        if self.site_codes_to_be_deprecated:
            self._validate_old_locations()
            self._validate_descendants_deprecated()
        self._validate_parents()
        self._validate_usernames()

    def _validate_old_locations(self):
        deprecating_locations_site_codes = (
            SQLLocation.active_objects
            .filter(domain=self.domain, site_code__in=self.site_codes_to_be_deprecated)
            .values_list('site_code', flat=True)
        )
        if len(deprecating_locations_site_codes) != len(self.site_codes_to_be_deprecated):
            self.errors.append(f"Found {len(deprecating_locations_site_codes)} locations for "
                               f"{len(self.site_codes_to_be_deprecated)} deprecating site codes")
        missing_site_codes = set(self.site_codes_to_be_deprecated) - set(deprecating_locations_site_codes)
        if missing_site_codes:
            self.errors.append(f"Could not find old locations with site codes {','.join(missing_site_codes)}")

    def _validate_descendants_deprecated(self):
        """
        ensure all locations getting deprecated, also have their descendants getting deprecated
        except for extract operation ensure at least one descendant getting deprecated
        """
        site_codes_to_be_deprecated = set(self.site_codes_to_be_deprecated)
        locations_to_be_deprecated_by_site_code = {
            loc.site_code: loc
            for loc in SQLLocation.active_objects.filter(
                domain=self.domain, site_code__in=self.site_codes_to_be_deprecated)
        }
        for transitions in self.valid_transitions.values():
            for transition in transitions:
                operation = transition.operation
                for old_site_code in transition.old_site_codes:
                    location = locations_to_be_deprecated_by_site_code.get(old_site_code)
                    # using an archived location's site code is already caught,
                    # but still adding another error for edge cases
                    if not location:
                        self.errors.append(f"Could not find old location with site code {old_site_code}")
                        continue
                    descendants_sites_codes = location.child_locations().values_list('site_code', flat=True)
                    if operation == EXTRACT_OPERATION:
                        if not set(descendants_sites_codes) & site_codes_to_be_deprecated:
                            self.errors.append(
                                f"Location {location.site_code} is getting deprecated via {operation} "
                                f"but none of its descendants")
                    else:
                        missing_site_codes = set(descendants_sites_codes) - site_codes_to_be_deprecated
                        if missing_site_codes:
                            self.errors.append(
                                f"Location {location.site_code} is getting deprecated via {operation} "
                                f"but the following descendants are not {', '.join(missing_site_codes)}")

    def _validate_parents(self):
        """
        validate new parent set respects the hierarchy
        if the parent location is already present in the system, validate it's location type and that its not
        getting archived
        else check for parent location in new locations to be created for the expected parent location type
        else add error for missing parent
        """
        for location_type_code in self.valid_transitions:
            expected_parent_type = self.location_type_parent.get(location_type_code)
            if not expected_parent_type:
                continue
            new_parent_site_codes = self._get_new_parent_site_codes(location_type_code)
            if not new_parent_site_codes:
                continue
            existing_new_parents = {
                loc.site_code: loc for loc in
                SQLLocation.active_objects.select_related('location_type')
                .filter(domain=self.domain, site_code__in=new_parent_site_codes)
            }
            for transition in self.valid_transitions[location_type_code]:
                for new_site_code, new_location_details in transition.new_location_details.items():
                    parent_site_code = new_location_details['parent_site_code']
                    if parent_site_code in existing_new_parents:
                        if existing_new_parents[parent_site_code].location_type.code != expected_parent_type:
                            self.errors.append(f"Unexpected parent {parent_site_code} "
                                               f"for type {location_type_code}")
                    elif parent_site_code not in self.new_site_codes_for_location_type[expected_parent_type]:
                        self.errors.append(f"Unexpected parent {parent_site_code} for type {location_type_code}")
                    if parent_site_code in self.site_codes_to_be_archived:
                        self.errors.append(f"Parent {parent_site_code} is marked for archival")

    def _get_new_parent_site_codes(self, location_type_code):
        parent_site_codes = set()
        for transition in self.valid_transitions[location_type_code]:
            for new_site_code, new_location_details in transition.new_location_details.items():
                if new_location_details['parent_site_code']:
                    parent_site_codes.add(new_location_details['parent_site_code'])
        return parent_site_codes

    def _validate_usernames(self):
        keys = [["active", self.domain, "CommCareUser", username] for username in self.usernames]
        result = CommCareUser.get_db().view(
            'users/by_domain',
            keys=keys,
            reduce=False,
            include_docs=False
        ).all()
        if len(result) != len(self.usernames):
            usernames_found = set([r['key'][-1] for r in result])
            usernames_missing = set(self.usernames) - usernames_found
            self.errors.append(f"Could not find user(s): {', '.join(usernames_missing)}")

    def valid_transitions_json(self, for_location_type=None):
        # return valid transitions as json
        json_response = {}
        for location_type, transitions in self.valid_transitions.items():
            if for_location_type and for_location_type != location_type:
                continue
            json_response[location_type] = [attr.asdict(transition) for transition in transitions]
        return json_response


class HouseholdReassignmentParser(object):
    def __init__(self, domain, workbook):
        self.domain = domain
        self.workbook = workbook
        self.reassignments = {}  # household id mapped to a dict with old_site_code and new_site_code

    def parse(self):
        errors = []
        for worksheet in self.workbook.worksheets:
            location_site_code = worksheet.title
            for row in worksheet:
                household_id = row.get(HOUSEHOLD_ID_COLUMN)
                new_awc_code = row.get(AWC_CODE_COLUMN)
                if not household_id:
                    errors.append("Missing Household ID for %s" % location_site_code)
                    continue
                if not new_awc_code:
                    errors.append("Missing New AWC Code for household ID %s" % household_id)
                    continue
                self.reassignments[household_id] = {
                    'old_site_code': location_site_code,
                    'new_site_code': new_awc_code
                }
        return errors
