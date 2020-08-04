from corehq.apps.users.util import filter_by_app


def get_app_version_used_by_user(app_id, user):
    last_build_details = get_reported_last_build_of_app_by_user(app_id, user)
    if last_build_details:
        return last_build_details.build_version


def get_reported_last_build_of_app_by_user(app_id, user):
    return filter_by_app(user.reporting_metadata.last_builds, app_id)
