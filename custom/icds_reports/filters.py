from datetime import datetime

import pytz

from corehq.apps.domain.models import Domain
from corehq.apps.hqwebapp.crispy import CSS_FIELD_CLASS, CSS_LABEL_CLASS
from corehq.apps.locations.models import SQLLocation
from corehq.apps.reports.filters.base import BaseSingleOptionFilter
from corehq.apps.reports.filters.fixtures import AsyncLocationFilter
from corehq.apps.reports.filters.select import MonthFilter, YearFilter
from custom.common.filters import RestrictedAsyncLocationFilter
from memoized import memoized


def location_hierarchy_config(domain, location_types=None):
    location_types = location_types or ['state', 'district', 'block']
    return [
        (loc_type.name, [loc_type.parent_type.name if loc_type.parent_type else None])
        for loc_type in Domain.get_by_name(
            domain
        ).location_types if loc_type.code in location_types
    ]


def load_restricted_locs(domain, selected_loc_id=None, user=None, show_test=False):

    def loc_to_json(loc):
        return {
            'name': loc.name,
            'location_type': loc.location_type.name,  # todo: remove when types aren't optional
            'uuid': loc.location_id,
            'is_archived': loc.is_archived,
            'can_edit': True
        }

    def _get_ancestor_loc_dict(location_list, location):
        for parent_location in location_list:
            if parent_location['uuid'] == location.location_id:
                return parent_location
        return None

    def location_transform(locations):
        loc_dict = dict()
        for loc in locations:
            if not show_test and loc.metadata.get('is_test_location', 'real') == 'test':
                continue
            parent_id = str(loc.parent_id)
            if parent_id in loc_dict:
                loc_dict[parent_id].append(loc)
            else:
                loc_dict[parent_id] = [loc]

        return loc_dict

    location_types = ['state', 'district', 'block', 'supervisor', 'awc']
    accessible_location = SQLLocation.objects.accessible_to_user(domain, user)
    location_level = 0

    if selected_loc_id:
        location = SQLLocation.by_location_id(selected_loc_id)
        location_level = location_types.index(location.location_type.name)
        lineage = location.get_ancestors()
        all_ancestors = [loc.id for loc in lineage] + [None]
        # to prevent all unnecessary decendent from being pulled
        accessible_location = accessible_location.filter(parent_id__in=all_ancestors)

    accessible_location = accessible_location.filter(
        location_type__name__in=location_types[: location_level + 1])
    parent_child_dict = location_transform(set(accessible_location).union(set(lineage)))
    locations_list = [loc_to_json(loc) for loc in parent_child_dict['None']]

    # if a location is selected, we need to pre-populate its location hierarchy
    # so that the data is available client-side to pre-populate the drop-downs
    if selected_loc_id:
        json_at_level = locations_list
        for loc in lineage:
            ancestor_loc_dict = _get_ancestor_loc_dict(json_at_level, loc)

            # could not find the ancestor at the level,
            # user should not have reached at this point to try and access an ancestor that is not permitted
            if ancestor_loc_dict is None:
                break

            children = parent_child_dict.get(str(loc.id), [])
            ancestor_loc_dict['children'] = [loc_to_json(loc) for loc in children]

            # reset level to one level down to find ancestor in next iteration
            json_at_level = ancestor_loc_dict['children']

    return locations_list


class ICDSTableauFilterMixin(object):
    def __init__(self, request, domain=None, timezone=pytz.utc, parent_report=None,
                 css_label=None, css_field=None):
        super(ICDSTableauFilterMixin, self).__init__(
            request, domain, timezone, parent_report, 'control-label ' + CSS_LABEL_CLASS, CSS_FIELD_CLASS
        )


class ICDSMonthFilter(ICDSTableauFilterMixin, MonthFilter):

    @property
    @memoized
    def selected(self):
        return self.get_value(self.request, self.domain) or "%02d" % datetime.now().month


class ICDSYearFilter(ICDSTableauFilterMixin, YearFilter):
    pass


class IcdsLocationFilter(AsyncLocationFilter):

    def load_locations_json(self, loc_id):
        show_test = self.request.GET.get('include_test', False)
        return load_restricted_locs(self.domain, loc_id, user=self.request.couch_user, show_test=show_test)


class IcdsRestrictedLocationFilter(IcdsLocationFilter):

    @property
    def location_hierarchy_config(self):
        return location_hierarchy_config(self.domain)


class TableauLocationFilter(ICDSTableauFilterMixin, RestrictedAsyncLocationFilter):

    auto_drill = False

    @property
    def location_hierarchy_config(self):
        return location_hierarchy_config(
            self.domain,
            location_types=['state', 'district', 'block', 'supervisor', 'awc']
        )


class CasteFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'caste'
    label = 'Caste'

    @property
    @memoized
    def selected(self):
        return super(CasteFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('ST', 'ST'),
            ('SC', 'SC'),
            ('OBC', 'OBC'),
            ('Others', 'Others'),
        ]


class MinorityFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'minority'
    label = 'Minority'

    @property
    @memoized
    def selected(self):
        return super(MinorityFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('Yes', 'Yes'),
            ('No', 'No')
        ]


class DisabledFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'disabled'
    label = 'Disabled'

    @property
    @memoized
    def selected(self):
        return super(DisabledFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('Yes', 'Yes'),
            ('No', 'No')
        ]


class ResidentFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'resident'
    label = 'Resident'

    @property
    @memoized
    def selected(self):
        return super(ResidentFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('Permanent', 'Permanent'),
            ('Resident', 'Resident')
        ]


class MaternalStatusFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'ccs_status'
    label = 'Maternal status'

    @property
    @memoized
    def selected(self):
        return super(MaternalStatusFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('Pregnant', 'Pregnant'),
            ('Lactating', 'Lactating')
        ]


class ChildAgeFilter(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'child_age_tranche'
    label = 'Child age'

    @property
    @memoized
    def selected(self):
        return super(ChildAgeFilter, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('0-28 days', '0-28 days'),
            ('28 days - 6mo', '28 days - 6mo'),
            ('1 yr', '1 yr'),
            ('2 yr', '2 yr'),
            ('3 yr', '3 yr'),
            ('4 yr', '4 yr'),
            ('5 yr', '5 yr'),
            ('6 yr', '6 yr'),
        ]


class THRBeneficiaryType(ICDSTableauFilterMixin, BaseSingleOptionFilter):
    slug = 'thr_beneficiary_type'
    label = 'THR Beneficiary Type'

    @property
    @memoized
    def selected(self):
        return super(THRBeneficiaryType, self).selected or self.options[0][0]

    @property
    def options(self):
        return [
            ('All', 'All'),
            ('Child', 'Child'),
            ('Pregnant', 'Pregnant'),
            ('Lactating', 'Lactating'),
        ]
