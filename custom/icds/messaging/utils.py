from corehq.apps.locations.dbaccessors import get_users_by_location_id
from corehq.apps.users.util import filter_by_app


def get_app_version_used_by_user(app_id, user):
    last_build_details = get_reported_last_build_of_app_by_user(app_id, user)
    if last_build_details:
        return last_build_details.build_version


def get_reported_last_build_of_app_by_user(app_id, user):
    return filter_by_app(user.reporting_metadata.last_builds, app_id)
def get_supervisor_for_aww(user):
    """
    Returns None if there is a misconfiguration (i.e., if the AWW's location
    has no parent location, or if there are no users at the parent location).
    """
    supervisor_location = user.sql_location.parent
    if supervisor_location is None:
        return None
    return get_users_by_location_id(user.domain, supervisor_location.location_id).first()
