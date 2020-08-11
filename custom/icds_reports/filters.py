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


# copy/paste from corehq.apps.location.utils
# added possibility to exclude test locations, test flag is custom added to the metadata in location object
def load_restricted_locs(domain, selected_loc_id=None, user=None, show_test=False):

    def loc_to_json(loc):
        return {
            'name': loc.name,
            'location_type': loc.location_type.name,  # todo: remove when types aren't optional
            'uuid': loc.location_id,
            'is_archived': loc.is_archived,
            'can_edit': True
        }

    def _get_accessible_location_ids(primary_location):
        accessible_location_ids = set([primary_location.location_id])
        accessible_location_ids.update(primary_location.get_ancestors().values_list('location_id', flat=True))
        accessible_location_ids.update(primary_location.get_descendants().values_list('location_id', flat=True))
        return accessible_location_ids

    def _get_ancestor_loc_dict(location_list, location):
        for parent_location in location_list:
            if parent_location['uuid'] == location.location_id:
                return parent_location
        return None

    user_locations = user.get_sql_locations(domain)
    user_has_all_location_access = user.has_permission(domain, 'access_all_locations')

    if user_has_all_location_access:
        accessible_root_locations = SQLLocation.root_locations(domain)
    else:
        accessible_root_locations = []
        for user_loc in user_locations:
            accessible_root_locations += list(user_loc
                                              .get_ancestors(include_self=True)
                                              .filter(parent_id__isnull=True))
        # select only unique roots, in case when user is assigned different locations of
        # same parent
        accessible_root_locations = list({loc.location_id: loc for loc in accessible_root_locations}.values())

    locations_list = [loc_to_json(loc) for loc in accessible_root_locations
                      if show_test or loc.metadata.get('is_test_location', 'real') != 'test'
                      ]

    # if a location is selected, we need to pre-populate its location hierarchy
    # so that the data is available client-side to pre-populate the drop-downs
    accessible_location_ids = set()
    if selected_loc_id:
        if not user_has_all_location_access:
            for user_loc in user_locations:
                accessible_location_ids.update(_get_accessible_location_ids(user_loc))

        selected = SQLLocation.objects.get(
            domain=domain,
            location_id=selected_loc_id
        )

        json_at_level = locations_list  # json in which we should find the ancestor in iteration
        for ancestor in selected.get_ancestors():  # this would start with top level ancestor first
            ancestor_loc_dict = _get_ancestor_loc_dict(json_at_level, ancestor)

            # could not find the ancestor at the level,
            # user should not have reached at this point to try and access an ancestor that is not permitted
            if ancestor_loc_dict is None:
                break

            child_locations = ancestor.get_children()

            ancestor_loc_dict['children'] = [
                loc_to_json(loc) for loc in child_locations
                if (show_test or loc.metadata.get('is_test_location', 'real') != 'test')
                and (user_has_all_location_access or loc.location_id in accessible_location_ids)
            ]

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
