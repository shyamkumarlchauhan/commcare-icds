from corehq.apps.app_manager.dbaccessors import get_build_by_version, wrap_app
from corehq.apps.locations.dbaccessors import get_users_by_location_id
from corehq.apps.users.util import filter_by_app
from corehq.util.quickcache import quickcache


def get_app_version_used_by_user(app_id, user):
    last_build_details = get_reported_last_build_of_app_by_user(app_id, user)
    if last_build_details:
        return last_build_details.build_version


def get_reported_last_build_of_app_by_user(app_id, user):
    return filter_by_app(user.reporting_metadata.last_builds, app_id)


@quickcache(['domain', 'app_id', 'version'], timeout=24 * 60 * 60, memoize_timeout=4 * 60 * 60)
def has_functional_version_set(domain, app_id, version):
    return bool(_get_build_functional_version(domain, app_id, version))


def _get_build_functional_version(domain, app_id, version):
    app_build = wrap_app(get_build_by_version(domain, app_id, version, return_doc=True))
    return app_build.profile.get('custom_properties', {}).get('cc-app-version-tag')


def get_supervisor_for_aww(user):
    """
    Returns None if there is a misconfiguration (i.e., if the AWW's location
    has no parent location, or if there are no users at the parent location).
    """
    supervisor_location = user.sql_location.parent
    if supervisor_location is None:
        return None
    return get_users_by_location_id(user.domain, supervisor_location.location_id).first()
