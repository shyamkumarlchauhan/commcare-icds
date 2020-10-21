import math
from datetime import date, datetime, timedelta

from django.conf import settings
from django.db import connections
from django.db.models import Max
from django.template.loader import get_template, render_to_string

import pytz
from lxml import etree
from memoized import memoized

from casexml.apps.phone.models import OTARestoreCommCareUser
from corehq.apps.app_manager.dbaccessors import (
    get_app,
    get_build_by_version,
    wrap_app,
)
from corehq.apps.app_manager.fixtures.mobile_ucr import (
    ReportFixturesProviderV1,
    ReportFixturesProviderV2,
)
from corehq.apps.userreports.models import StaticDataSourceConfiguration
from corehq.apps.userreports.util import get_table_name
from corehq.sql_db.connections import connection_manager
from corehq.util.quickcache import quickcache
from corehq.util.soft_assert import soft_assert
from custom.icds.const import (
    CHILDREN_WEIGHED_REPORT_ID,
    DAYS_AWC_OPEN_REPORT_ID,
    HOME_VISIT_REPORT_ID,
    SUPERVISOR_APP_ID,
    THR_REPORT_ID,
    UCR_V2_AG_ALIAS,
    UCR_V2_AG_MONTHLY_ALIAS,
    UCR_V2_CBE_LAST_MONTH_ALIAS,
    UCR_V2_LS_DAYS_AWC_OPEN_ALIAS,
    UCR_V2_LS_TIMELY_HOME_VISITS_ALIAS,
    UCR_V2_MPR_5_CCS_RECORD_ALIAS,
    UCR_V2_MPR_5_CHILD_HEALTH_CASES_MONTHLY_ALIAS,
    UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS,
)
from custom.icds.messaging.utils import (
    get_app_version_used_by_user,
    get_supervisor_for_aww,
)
from custom.icds_reports.cache import icds_quickcache
from custom.icds_reports.models.aggregate import AggregateInactiveAWW
from dimagi.utils.couch import CriticalSection

REPORT_IDS = [
    HOME_VISIT_REPORT_ID,
    THR_REPORT_ID,
    CHILDREN_WEIGHED_REPORT_ID,
    DAYS_AWC_OPEN_REPORT_ID,
]

REPORT_ALIASES = [
    UCR_V2_AG_ALIAS,
    UCR_V2_AG_MONTHLY_ALIAS,
    UCR_V2_CBE_LAST_MONTH_ALIAS,
    UCR_V2_LS_DAYS_AWC_OPEN_ALIAS,
    UCR_V2_MPR_5_CCS_RECORD_ALIAS,
    UCR_V2_MPR_5_CHILD_HEALTH_CASES_MONTHLY_ALIAS,
    UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS,
    UCR_V2_LS_TIMELY_HOME_VISITS_ALIAS,
]


@quickcache(['domain', 'ls_app_version'], timeout=4 * 60 * 60, memoize_timeout=4 * 60 * 60)
def get_report_configs(domain, ls_app_version):
    if ls_app_version:
        app = wrap_app(get_build_by_version(domain, SUPERVISOR_APP_ID, ls_app_version, return_doc=True))
    else:
        app = get_app(domain, SUPERVISOR_APP_ID, latest=True)
    return {
        report_config.report_id: report_config
        for module in app.get_report_modules()
        for report_config in module.report_configs
        if report_config.report_id in REPORT_IDS
    }


@quickcache(['domain', 'report_id', 'ota_user.user_id', 'ls_app_version'], timeout=12 * 60 * 60)
def _get_cached_report_fixture_for_user(domain, report_id, ota_user, ls_app_version):
    """
    :param domain: the domain
    :param report_id: the index to the result from get_report_configs()
    :param ota_user: the OTARestoreCommCareUser for which to get the report fixture
    :param ls_app_version: the version of the app ls is on
    """
    [xml] = ReportFixturesProviderV1().report_config_to_fixture(
        get_report_configs(domain, ls_app_version)[report_id], ota_user
    )
    return etree.tostring(xml)


def get_report_fixture_for_user(domain, report_id, ota_user, ls_app_version):
    """
    The Element objects used by the lxml library don't cache properly.
    So instead we cache the XML string and convert back here.
    """
    return etree.fromstring(_get_cached_report_fixture_for_user(domain, report_id, ota_user, ls_app_version))


def get_v2_report_fixture_for_user(domain, report_slug, ota_user, ls_app_version):
    """
    The Element objects used by the lxml library don't cache properly.
    So instead we cache the XML string and convert back here.
    """
    return etree.fromstring(_get_cached_v2_report_fixture_for_user(domain, report_slug, ota_user, ls_app_version))


@quickcache(['domain', 'report_slug', 'ota_user.user_id', 'ls_app_version'], timeout=12 * 60 * 60)
def _get_cached_v2_report_fixture_for_user(domain, report_slug, ota_user, ls_app_version):
    """
    :param domain: the domain
    :param report_slug: the slug/alias of the report
    :param ota_user: the OTARestoreCommCareUser for which to get the report fixture
    :param ls_app_version: the version of app user is on
    """
    report_config = _get_v2_report_configs(domain, ls_app_version)[report_slug]
    xml = ReportFixturesProviderV2().report_config_to_fixture(
        report_config, ota_user
    )[1]
    return etree.tostring(xml)


@quickcache(['domain', 'ls_app_version'], timeout=4 * 60 * 60, memoize_timeout=4 * 60 * 60)
def _get_v2_report_configs(domain, ls_app_version):
    if ls_app_version:
        app = wrap_app(get_build_by_version(domain, SUPERVISOR_APP_ID, ls_app_version, return_doc=True))
    else:
        app = get_app(domain, SUPERVISOR_APP_ID, latest=True)
    return {
        report_config.report_slug: report_config
        for module in app.get_report_modules()
        for report_config in module.report_configs
        if report_config.report_slug in REPORT_ALIASES
    }


class IndicatorError(Exception):
    pass


class SMSIndicator(object):
    # The name of the template to be used when rendering the message(s) to be sent
    # This should not be the full path, just the name of the file, since the path
    # is determined at runtime by language code
    template = None

    # The domain this indicator is being run in
    domain = None

    # The user the indicator is being run for
    # For AWWIndicator, this is the AWW CommCareUser
    # For LSIndicator, this is the LS CommCareUser
    user = None

    # This is used to identify the indicator in the SMS data
    slug = None

    def __init__(self, domain, user):
        self.domain = domain
        self.user = user

    @property
    @memoized
    def timezone(self):
        return pytz.timezone('Asia/Kolkata')

    @property
    @memoized
    def now(self):
        return datetime.now(tz=self.timezone)

    def get_messages(self, language_code):
        """
        Should return a list of messages that should be sent.
        """
        raise NotImplementedError()

    def render_template(self, context, language_code):
        return self._render_template(context, language_code)

    def _render_template(self, context, language_code):
        template_path = self._template_path(language_code)
        return render_to_string(template_path, context).strip()

    def _template_path(self, language_code):
        return 'icds/messaging/indicators/%s/%s' % (language_code, self.template)


class AWWIndicator(SMSIndicator):
    @property
    @memoized
    def supervisor(self):
        return get_supervisor_for_aww(self.user)


class LSIndicator(SMSIndicator):
    @property
    @memoized
    def child_locations(self):
        return self.user.sql_location.child_locations()

    @property
    @memoized
    def awc_locations(self):
        return {l.location_id: l.name for l in self.child_locations}


class BaseAWWAggregatePerformanceIndicator(AWWIndicator):
    def get_messages(self, language_code):
        raise NotImplementedError()


class AWWAggregatePerformanceIndicator(BaseAWWAggregatePerformanceIndicator):
    template = 'aww_aggregate_performance.txt'
    slug = 'aww_2'

    def get_value_from_fixture(self, fixture, attribute):
        xpath = './rows/row[@is_total_row="False"]'
        rows = fixture.findall(xpath)
        location_name = self.user.sql_location.name
        for row in rows:
            owner_id = row.find('./column[@id="owner_id"]')
            if owner_id.text == location_name:
                try:
                    return row.find('./column[@id="{}"]'.format(attribute)).text
                except:
                    raise IndicatorError(
                        "Attribute {} not found in restore for AWC {}".format(attribute, location_name)
                    )

        return 0

    def get_messages(self, language_code):
        if self.supervisor is None:
            return []

        agg_perf = LSAggregatePerformanceIndicator(self.domain, self.supervisor)

        visits = self.get_value_from_fixture(agg_perf.visits_fixture, 'count')
        on_time_visits = self.get_value_from_fixture(agg_perf.visits_fixture, 'visit_on_time')
        thr_gte_21 = self.get_value_from_fixture(agg_perf.thr_fixture, 'open_ccs_thr_gte_21')
        thr_count = self.get_value_from_fixture(agg_perf.thr_fixture, 'open_count')
        num_weigh = self.get_value_from_fixture(agg_perf.weighed_fixture, 'open_weighed')
        num_weigh_avail = self.get_value_from_fixture(agg_perf.weighed_fixture, 'open_count')
        num_days_open = self.get_value_from_fixture(agg_perf.days_open_fixture, 'awc_opened_count')

        context = {
            "visits": visits,
            "on_time_visits": on_time_visits,
            "thr_distribution": "{} / {}".format(thr_gte_21, thr_count),
            "children_weighed": "{} / {}".format(num_weigh, num_weigh_avail),
            "days_open": num_days_open,
        }

        return [self.render_template(context, language_code=language_code)]


class AWWAggregatePerformanceIndicatorV2(BaseAWWAggregatePerformanceIndicator):
    template = 'aww_aggregate_performance_v2.txt'
    slug = 'aww_v2'

    def get_value_from_fixture(self, fixture, attribute):
        xpath = './rows/row[@is_total_row="False"]'
        rows = fixture.findall(xpath)
        location_name = self.user.sql_location.name
        last_month_string = _get_last_month_string()
        for row in rows:
            owner_id = row.find('owner_id')
            month = row.find('month')
            if owner_id.text == location_name and month is not None and month.text == last_month_string:
                try:
                    return row.find(attribute).text
                except:
                    raise IndicatorError(
                        f"Attribute {attribute} not found in restore for AWC {location_name}"
                    )
        return 0

    def get_rows_count_from_fixture(self, fixture):
        count = 0
        xpath = './rows/row[@is_total_row="False"]'
        rows = fixture.findall(xpath)
        location_name = self.user.sql_location.name
        for row in rows:
            owner_id = row.find("awc_id")
            if owner_id.text == location_name:
                count += 1
        return count

    def get_messages(self, language_code):
        get_template(self._template_path(language_code))  # fail early if template missing
        if self.supervisor is None:
            return []

        ls_agg_perf_indicator = LSAggregatePerformanceIndicatorV2(self.domain, self.supervisor)
        data = _get_data_for_v2_performance_indicator(self, ls_agg_perf_indicator)
        return [self.render_template(data, language_code=language_code)]


# All process_sms tasks should hopefully be finished in 4 hours
@quickcache([], timeout=60 * 60 * 4)
def is_aggregate_inactive_aww_data_fresh(send_email=False):
    # Heuristic to check if collect_inactive_awws task ran succesfully today or yesterday
    #   This would return False if both today and yesterday's task failed
    #   or if the last-submission is older than a day due to pillow lag.
    last_submission = AggregateInactiveAWW.objects.filter(
        last_submission__isnull=False
    ).aggregate(Max('last_submission'))['last_submission__max']
    if not last_submission:
        return False
    is_fresh = last_submission >= (datetime.today() - timedelta(days=1)).date()
    SMS_TEAM = ['{}@{}'.format('icds-sms-rule', 'dimagi.com')]
    if not send_email:
        return is_fresh
    _soft_assert = soft_assert(to=SMS_TEAM, send_to_ops=False)
    if is_fresh:
        _soft_assert(False, "The weekly inactive SMS rule is successfully triggered for this week")
    else:
        _soft_assert(False,
            "The weekly inactive SMS rule is skipped for this week as latest_submission {} data is older than one day"
            .format(str(last_submission))
        )
    return is_fresh


class AWWSubmissionPerformanceIndicator(AWWIndicator):
    template = 'aww_no_submissions.txt'
    last_submission_date = None
    slug = 'aww_1'

    def __init__(self, domain, user):
        super(AWWSubmissionPerformanceIndicator, self).__init__(domain, user)

        result = AggregateInactiveAWW.objects.filter(
            awc_id=user.location_id).values('last_submission').first()
        if result:
            self.last_submission_date = result['last_submission']

    def get_messages(self, language_code):
        if not is_aggregate_inactive_aww_data_fresh(send_email=True):
            return []

        more_than_one_week = False
        more_than_one_month = False
        one_month_ago = (datetime.utcnow() - timedelta(days=30)).date()
        one_week_ago = (datetime.utcnow() - timedelta(days=7)).date()

        if not self.last_submission_date or self.last_submission_date < one_month_ago:
            more_than_one_month = True
        elif self.last_submission_date < one_week_ago:
            more_than_one_week = True

        if more_than_one_week or more_than_one_month:
            context = {
                'more_than_one_week': more_than_one_week,
                'more_than_one_month': more_than_one_month,
                'awc': self.user.sql_location.name,
            }
            return [self.render_template(context, language_code=language_code)]

        return []


class AWWVHNDSurveyIndicator(AWWIndicator):
    template = 'aww_vhnd_survey.txt'
    slug = 'phase2_aww_1'

    def __init__(self, domain, user):
        super(AWWVHNDSurveyIndicator, self).__init__(domain, user)

        self.should_send_sms = bool(get_awcs_with_old_vhnd_date(domain, [self.user.location_id]))

    def get_messages(self, language_code):
        if self.should_send_sms:
            return [self.render_template({}, language_code=language_code)]
        else:
            return []


def _get_last_submission_dates(awc_ids):
    return {
        row['awc_id']: row['last_submission']
        for row in AggregateInactiveAWW.objects.filter(
        awc_id__in=awc_ids).values('awc_id', 'last_submission').all()
    }


class LSSubmissionPerformanceIndicator(LSIndicator):
    template = 'ls_no_submissions.txt'
    slug = 'ls_6'

    def __init__(self, domain, user):
        super(LSSubmissionPerformanceIndicator, self).__init__(domain, user)

        self.last_submission_dates = _get_last_submission_dates(awc_ids=set(self.awc_locations))

    def get_messages(self, language_code):
        messages = []
        one_week_loc_ids = []
        one_month_loc_ids = []

        now_date = self.now.date()
        for loc_id in self.awc_locations:
            last_sub_date = self.last_submission_dates.get(loc_id)
            if not last_sub_date:
                one_month_loc_ids.append(loc_id)
            else:
                days_since_submission = (now_date - last_sub_date).days
                if days_since_submission > 7:
                    one_week_loc_ids.append(loc_id)

        if one_week_loc_ids:
            one_week_loc_names = {self.awc_locations[loc_id] for loc_id in one_week_loc_ids}
            week_context = {'location_names': ', '.join(one_week_loc_names), 'timeframe': 'week'}
            messages.append(self.render_template(week_context, language_code=language_code))

        if one_month_loc_ids:
            one_month_loc_names = {self.awc_locations[loc_id] for loc_id in one_month_loc_ids}
            month_context = {'location_names': ','.join(one_month_loc_names), 'timeframe': 'month'}
            messages.append(self.render_template(month_context, language_code=language_code))

        return messages


class LSVHNDSurveyIndicator(LSIndicator):
    template = 'ls_vhnd_survey.txt'
    slug = 'ls_2'

    def __init__(self, domain, user):
        super(LSVHNDSurveyIndicator, self).__init__(domain, user)

        self.awc_ids_not_in_timeframe = get_awcs_with_old_vhnd_date(
            domain,
            set(self.awc_locations)
        )

    def get_messages(self, language_code):
        messages = []

        if self.awc_ids_not_in_timeframe:
            awc_names = {self.awc_locations[awc] for awc in self.awc_ids_not_in_timeframe}
            context = {'location_names': ', '.join(awc_names)}
            messages.append(self.render_template(context, language_code=language_code))

        return messages


def get_awcs_with_old_vhnd_date(domain, awc_location_ids):
    return set(awc_location_ids) - get_awws_in_vhnd_timeframe(domain)


@icds_quickcache(
    ['domain'], timeout=12 * 60 * 60, memoize_timeout=12 * 60 * 60, session_function=None,
    skip_arg=lambda *args: settings.UNIT_TESTING)
def get_awws_in_vhnd_timeframe(domain):
    # This function is called concurrently by many tasks.
    # The CriticalSection ensures that the expensive operation is not triggered
    #   by each task again, the compute_awws_in_vhnd_timeframe itself is cached separately,
    #   so that other waiting tasks could lookup the value from cache.
    with CriticalSection(['compute_awws_in_vhnd_timeframe']):
        return compute_awws_in_vhnd_timeframe(domain)


@icds_quickcache(
    ['domain'], timeout=60 * 60, memoize_timeout=60 * 60, session_function=None,
    skip_arg=lambda *args: settings.UNIT_TESTING)
def compute_awws_in_vhnd_timeframe(domain):
    """
    This computes awws with vhsnd_date_past_month less than 37 days.

    Result is cached in local memory, so that indvidual reminder tasks
    per AWW/LS dont hit the database each time
    """
    table = get_table_name(domain, 'static-vhnd_form')
    query = """
    SELECT DISTINCT awc_id
    FROM "{table}"
    WHERE vhsnd_date_past_month > %(37_days_ago)s
    """.format(table=table)
    cutoff = datetime.now(tz=pytz.timezone('Asia/Kolkata')).date()
    query_params = {"37_days_ago": cutoff - timedelta(days=37)}

    datasource_id = StaticDataSourceConfiguration.get_doc_id(domain, 'static-vhnd_form')
    data_source = StaticDataSourceConfiguration.by_id(datasource_id)
    django_db = connection_manager.get_django_db_alias(data_source.engine_id)
    with connections[django_db].cursor() as cursor:
        cursor.execute(query, query_params)
        return {row[0] for row in cursor.fetchall()}


class BaseLSAggregatePerformanceIndicator(LSIndicator):
    @property
    @memoized
    def restore_user(self):
        return OTARestoreCommCareUser(self.domain, self.user)

    def get_messages(self, language_code):
        pass


class LSAggregatePerformanceIndicator(BaseLSAggregatePerformanceIndicator):
    template = 'ls_aggregate_performance.txt'
    slug = 'ls_1'

    def __init__(self, domain, user):
        super().__init__(domain, user)
        self.app_version = get_app_version_used_by_user(SUPERVISOR_APP_ID, user)

    def get_report_fixture(self, report_id):
        return get_report_fixture_for_user(self.domain, report_id, self.restore_user, self.app_version)

    @property
    @memoized
    def visits_fixture(self):
        return self.get_report_fixture(HOME_VISIT_REPORT_ID)

    @property
    @memoized
    def thr_fixture(self):
        return self.get_report_fixture(THR_REPORT_ID)

    @property
    @memoized
    def weighed_fixture(self):
        return self.get_report_fixture(CHILDREN_WEIGHED_REPORT_ID)

    @property
    @memoized
    def days_open_fixture(self):
        return self.get_report_fixture(DAYS_AWC_OPEN_REPORT_ID)

    def get_value_from_fixture(self, fixture, attribute):
        xpath = './rows/row[@is_total_row="True"]/column[@id="{}"]'.format(attribute)
        try:
            return fixture.findall(xpath)[0].text
        except:
            raise IndicatorError("{} not found in fixture {} for user {}".format(
                attribute, fixture, self.user.get_id
            ))

    def get_messages(self, language_code):
        on_time_visits = self.get_value_from_fixture(self.visits_fixture, 'visit_on_time')
        visits = self.get_value_from_fixture(self.visits_fixture, 'count')
        thr_gte_21 = self.get_value_from_fixture(self.thr_fixture, 'open_ccs_thr_gte_21')
        thr_count = self.get_value_from_fixture(self.thr_fixture, 'open_count')
        num_weigh = self.get_value_from_fixture(self.weighed_fixture, 'open_weighed')
        num_weigh_avail = self.get_value_from_fixture(self.weighed_fixture, 'open_count')
        num_days_open = int(self.get_value_from_fixture(self.days_open_fixture, 'awc_opened_count'))
        num_awc_locations = len(self.awc_locations)
        if num_awc_locations:
            avg_days_open = int(round(num_days_open / num_awc_locations))
        else:
            # catch div by 0
            avg_days_open = 0

        context = {
            "on_time_visits": on_time_visits,
            "visits": visits,
            "visits_goal": num_awc_locations * 65,
            "thr_distribution": "{} / {}".format(thr_gte_21, thr_count),
            "children_weighed": "{} / {}".format(num_weigh, num_weigh_avail),
            "days_open": "{}".format(avg_days_open),
        }

        return [self.render_template(context, language_code=language_code)]


class LSAggregatePerformanceIndicatorV2(BaseLSAggregatePerformanceIndicator):
    template = 'ls_aggregate_performance_v2.txt'
    slug = 'ls_v2'

    def __init__(self, domain, user):
        super().__init__(domain, user)
        self.app_version = get_app_version_used_by_user(SUPERVISOR_APP_ID, user)

    @memoized
    def get_report_fixture(self, report_id):
        return get_v2_report_fixture_for_user(self.domain, report_id, self.restore_user, self.app_version)

    def get_value_from_fixture(self, fixture, attribute):
        xpath = './rows/row[@is_total_row="False"]'
        rows = fixture.findall(xpath)
        last_month_string = _get_last_month_string()
        total = 0
        for row in rows:
            month = row.find('month')
            if month is not None and month.text == last_month_string:
                try:
                    total += int(row.find(attribute).text)
                except:
                    raise IndicatorError(f"{attribute} not found in fixture {fixture} for user {self.user.get_id}")
        return total

    @staticmethod
    def get_rows_count_from_fixture(fixture):
        xpath = './rows/row[@is_total_row="False"]'
        rows = fixture.findall(xpath)
        return len(rows)

    def get_messages(self, language_code):
        get_template(self._template_path(language_code))  # fail early if template missing
        data = _get_data_for_v2_performance_indicator(self, self)
        num_awc_locations = len(self.awc_locations)
        num_days_open = int(data.pop('num_days_open'))

        avg_days_open = 0
        if num_awc_locations:
            avg_days_open = int(round(num_days_open / num_awc_locations))

        data["avg_days_open"] = avg_days_open
        return [self.render_template(data, language_code=language_code)]


def _get_data_for_v2_performance_indicator(indicator_obj, ls_indicator_obj):
    data = {}
    for store_as, report_slug, column_name in v2_indicator_data_points:
        fixture = ls_indicator_obj.get_report_fixture(report_slug)
        data[store_as] = int(indicator_obj.get_value_from_fixture(fixture, column_name))

    data["visits_goal"] = math.ceil(
        (data.pop("count_bp") * 0.44) + data.pop("count_ebf")
        + (data.pop("count_pnc") * 6) + (data.pop("count_cf") * 0.39)
    )
    data["ccs_gte_21"] = data.pop("ccs_thr_rations_gte_21") + data.pop("child_health_thr_rations_gte_21")
    data["ccs_total"] = data.pop("ccs_open_in_month") + data.pop("child_health_open_in_month")

    cbe_report_fixture = ls_indicator_obj.get_report_fixture(UCR_V2_CBE_LAST_MONTH_ALIAS)
    data["cbe_conducted"] = indicator_obj.get_rows_count_from_fixture(cbe_report_fixture)
    return data


def _get_last_month_string():
    today = date.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.strftime("%Y-%m")


v2_indicator_data_points = (
    # store as, report alias, column name
    ("visits", UCR_V2_LS_TIMELY_HOME_VISITS_ALIAS, "count"),
    ("count_bp", UCR_V2_MPR_5_CCS_RECORD_ALIAS, "count_bp"),
    ("count_ebf", UCR_V2_MPR_5_CCS_RECORD_ALIAS, "count_ebf"),
    ("count_cf", UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS, "count_cf"),
    ("count_pnc", UCR_V2_MPR_5_CCS_RECORD_ALIAS, "count_pnc"),
    ("on_time_visits", UCR_V2_LS_TIMELY_HOME_VISITS_ALIAS, "visit_on_time"),
    ("ccs_thr_rations_gte_21", UCR_V2_MPR_5_CCS_RECORD_ALIAS, "thr_rations_gte_21"),
    ("child_health_thr_rations_gte_21", UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS, "thr_rations_gte_21"),
    ("ccs_open_in_month", UCR_V2_MPR_5_CCS_RECORD_ALIAS, "open_in_month"),
    ("child_health_open_in_month", UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS, "open_in_month"),
    ("weighed_in_month", UCR_V2_MPR_5_CHILD_HEALTH_CASES_MONTHLY_ALIAS, "weighed_in_month"),
    ("open_in_month", UCR_V2_MPR_5_CHILD_HEALTH_CASES_MONTHLY_ALIAS, "open_in_month"),
    ("num_days_open", UCR_V2_LS_DAYS_AWC_OPEN_ALIAS, "awc_opened_count"),
    ("hcm_21_plus_days", UCR_V2_AG_MONTHLY_ALIAS, "hcm_21_plus_days"),
    ("thr_21_plus_days", UCR_V2_AG_MONTHLY_ALIAS, "thr_21_plus_days"),
    ("total_ag_oos", UCR_V2_AG_ALIAS, "out_of_school"),
)
