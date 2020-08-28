from corehq.apps.es import filters
from corehq.apps.locations.models import SQLLocation
from corehq.util.quickcache import quickcache
from custom.icds_core.const import CPMU_ROLE_NAME
from corehq.apps.es.users import location as location_filter


def filter_users_in_test_locations(couch_user, domain, user_query):
    if couch_user.get_role(domain).name == CPMU_ROLE_NAME:
        test_location_ids = find_test_location_ids(domain)
        user_query = user_query.filter(
            filters.NOT(
                location_filter(test_location_ids)
            )
        )
    return user_query


@quickcache(['domain'], timeout=7 * 24 * 60 * 60)
def find_test_location_ids(domain):
    test_location_ids = set()
    TEST_STATES = []
    for loc in SQLLocation.active_objects.filter(location_type__code='state', domain=domain):
        if loc.metadata.get('is_test_location') == 'test':
            TEST_STATES.append(loc.name)
    for location in SQLLocation.active_objects.filter(name__in=TEST_STATES, domain=domain):
        test_location_ids.update(location.get_descendants(include_self=True).values_list('location_id', flat=True))
    return test_location_ids
