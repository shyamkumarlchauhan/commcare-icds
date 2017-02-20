from collections import defaultdict
from datetime import datetime, timedelta

import pytz

from django.template import TemplateDoesNotExist
from django.template.loader import render_to_string

from casexml.apps.phone.models import OTARestoreCommCareUser
from dimagi.utils.dates import DateSpan
from dimagi.utils.decorators.memoized import memoized
from dimagi.utils.parsing import string_to_datetime

from corehq.apps.app_manager.dbaccessors import get_app
from corehq.apps.app_manager.fixtures.mobile_ucr import ReportFixturesProvider
from corehq.apps.locations.dbaccessors import (
    get_user_ids_from_primary_location_ids,
    get_users_location_ids,
    get_users_by_location_id,
)
from corehq.apps.locations.models import SQLLocation
from corehq.apps.reports.analytics.esaccessors import (
    get_last_submission_time_for_users,
    get_last_form_submissions_by_user,
)
from custom.icds.const import (
    CHILDREN_WEIGHED_REPORT_ID,
    DAYS_AWC_OPEN_REPORT_ID,
    HOME_VISIT_REPORT_ID,
    SUPERVISOR_APP_ID,
    THR_REPORT_ID,
    VHND_SURVEY_XMLNS,
)

DEFAULT_LANGUAGE = 'hin'


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

    def get_messages(self, language_code=None):
        """
        Should return a list of messages that should be sent.
        """
        raise NotImplementedError()

    def render_template(self, context, language_code=None):
        if not language_code:
            return self._render_template(context, DEFAULT_LANGUAGE)

        try:
            return self._render_template(context, language_code)
        except TemplateDoesNotExist:
            return self._render_template(context, DEFAULT_LANGUAGE)

    def _render_template(self, context, language_code):
        template_name = 'icds/messaging/indicators/%s/%s' % (language_code, self.template)
        return render_to_string(template_name, context).strip()


class AWWIndicator(SMSIndicator):
    @property
    @memoized
    def supervisor(self):
        supervisor_location = self.user.sql_location.parent
        return get_users_by_location_id(self.domain, supervisor_location.location_id).first()


class LSIndicator(SMSIndicator):
    @property
    @memoized
    def child_locations(self):
        return self.user.sql_location.child_locations()

    @property
    @memoized
    def awc_locations(self):
        return {l.location_id: l.name for l in self.child_locations}

    @property
    @memoized
    def locations_by_user_id(self):
        return get_user_ids_from_primary_location_ids(self.domain, set(self.awc_locations))

    @property
    @memoized
    def aww_user_ids(self):
        return set(self.locations_by_user_id.keys())

    @property
    @memoized
    def user_ids_by_location_id(self):
        user_ids_by_location_id = defaultdict(set)
        for u_id, loc_id in self.locations_by_user_id.items():
            user_ids_by_location_id[loc_id].add(u_id)

        return user_ids_by_location_id

    def location_names_from_user_id(self, user_ids):
        loc_ids = {self.locations_by_user_id.get(u) for u in user_ids}
        return {self.awc_locations[l] for l in loc_ids}


class AWWAggregatePerformanceIndicator(AWWIndicator):
    def get_messages(self, language_code=None):
        return LSAggregatePerformanceIndicator(self.domain, self.supervisor).get_messages(language_code)


class AWWSubmissionPerformanceIndicator(AWWIndicator):
    template = 'aww_no_submissions.txt'
    last_submission_date = None

    def __init__(self, domain, user):
        super(AWWSubmissionPerformanceIndicator, self).__init__(domain, user)

        result = get_last_submission_time_for_users(self.domain, [self.user.get_id], self.get_datespan())
        self.last_submission_date = result.get(self.user.get_id)

    def get_datespan(self):
        today = datetime(self.now.year, self.now.month, self.now.day)
        end_date = today + timedelta(days=1)
        start_date = today - timedelta(days=30)
        return DateSpan(start_date, end_date, timezone=self.timezone)

    def get_messages(self, language_code=None):
        more_than_one_week = False
        more_than_one_month = False

        if not self.last_submission_date:
            more_than_one_month = True
        else:
            days_since_submission = (self.now.date() - self.last_submission_date).days
            if days_since_submission > 7:
                more_than_one_week = True

        if more_than_one_week or more_than_one_month:
            context = {
                'more_than_one_week': more_than_one_week,
                'more_than_one_month': more_than_one_month,
                'awc': self.user.sql_location.name,
            }
            return [self.render_template(context, language_code=language_code)]

        return []


class LSSubmissionPerformanceIndicator(LSIndicator):
    template = 'ls_no_submissions.txt'

    def __init__(self, domain, user):
        super(LSSubmissionPerformanceIndicator, self).__init__(domain, user)

        self.last_submission_dates = get_last_submission_time_for_users(
            self.domain, self.aww_user_ids, self.get_datespan()
        )

    def get_datespan(self):
        today = datetime(self.now.year, self.now.month, self.now.day)
        end_date = today + timedelta(days=1)
        start_date = today - timedelta(days=30)
        return DateSpan(start_date, end_date, timezone=self.timezone)

    def get_messages(self, language_code=None):
        messages = []
        one_week_user_ids = []
        one_month_user_ids = []

        now_date = self.now.date()
        for user_id in self.aww_user_ids:
            last_sub_date = self.last_submission_dates.get(user_id)
            if not last_sub_date:
                one_month_user_ids.append(user_id)
            else:
                days_since_submission = (now_date - last_sub_date).days
                if days_since_submission > 7:
                    one_week_user_ids.append(user_id)

        if one_week_user_ids:
            one_week_loc_names = self.location_names_from_user_id(one_week_user_ids)
            week_context = {'location_names': ', '.join(one_week_loc_names), 'timeframe': 'week'}
            messages.append(self.render_template(week_context, language_code=language_code))

        if one_month_user_ids:
            one_month_loc_names = self.location_names_from_user_id(one_month_user_ids)
            month_context = {'location_names': ','.join(one_month_loc_names), 'timeframe': 'month'}
            messages.append(self.render_template(month_context, language_code=language_code))

        return messages


class LSVHNDSurveyIndicator(LSIndicator):
    template = 'ls_vhnd_survey.txt'

    def __init__(self, domain, user):
        super(LSVHNDSurveyIndicator, self).__init__(domain, user)

        self.forms = get_last_form_submissions_by_user(
            domain, self.aww_user_ids, xmlns=VHND_SURVEY_XMLNS
        )

    def get_messages(self, language_code=None):
        def convert_to_date(date):
            return string_to_datetime(date).date() if date else None

        now_date = self.now.date()
        user_ids_with_forms_in_time_frame = set()
        for user_id, form in self.forms.items():
            vhnd_date = convert_to_date(form['form']['vhsnd_date_planned'])
            if (now_date - vhnd_date).days < 37:
                user_ids_with_forms_in_time_frame.add(user_id)

        awc_ids = {
            loc
            for loc, user_ids in self.user_ids_by_location_id.items()
            if user_ids.isdisjoint(user_ids_with_forms_in_time_frame)
        }
        messages = []

        if awc_ids:
            awc_names = {self.awc_locations[awc] for awc in awc_ids}
            context = {'location_names': ', '.join(awc_names)}
            messages.append(self.render_template(context, language_code=language_code))

        return messages


class LSAggregatePerformanceIndicator(LSIndicator):
    template = 'ls_aggregate_performance.txt'

    @property
    @memoized
    def restore_user(self):
        return OTARestoreCommCareUser(self.domain, self.user)

    @property
    @memoized
    def report_configs(self):
        report_ids = [
            HOME_VISIT_REPORT_ID,
            THR_REPORT_ID,
            CHILDREN_WEIGHED_REPORT_ID,
            DAYS_AWC_OPEN_REPORT_ID,
        ]
        app = get_app(self.domain, SUPERVISOR_APP_ID)
        return {
            report_config.report_id: report_config
            for module in app.modules if isinstance(module, ReportModule)
            for report_config in module.report_configs
            if report_config.report_id in report_ids
        }

    def get_report_fixture(self, report_id):
        return ReportFixturesProvider.report_config_to_fixture(
            self.report_configs[report_id], self.restore_user
        )

    @property
    def visits_fixture(self):
        return self.get_report_fixture(HOME_VISIT_REPORT_ID)

    @property
    def thr_fixture(self):
        return self.get_report_fixture(THR_REPORT_ID)

    @property
    def weighed_fixture(self):
        return self.get_report_fixture(CHILDREN_WEIGHED_REPORT_ID)

    @property
    def days_open_fixture(self):
        return self.get_report_fixture(DAYS_AWC_OPEN_REPORT_ID)

    def get_value_from_fixture(self, fixture, attribute):
        xpath = './rows/row[@is_total_row="True"]/column[@id="{}"]'.format(attribute)
        return fixture.findall(xpath)[0].text

    def get_messages(self, language_code=None):
        visits = self.get_value_from_fixture(self.visits_fixture, 'count')
        thr_gte_21 = self.get_value_from_fixture(self.thr_fixture, 'open_ccs_thr_gte_21')
        thr_count = self.get_value_from_fixture(self.thr_fixture, 'open_count')
        num_weigh = self.get_value_from_fixture(self.weighed_fixture, 'open_weighed')
        num_weigh_avail = self.get_value_from_fixture(self.weighed_fixture, 'open_count')
        num_days_open = self.get_value_from_fixture(self.days_open_fixture, 'awc_opened_count')

        context = {
            "visits": "{}/65".format(visits),
            "thr_distribution": "{} / {}".format(thr_gte_21, thr_count),
            "children_weighed": "{} / {}".format(num_weigh, num_weigh_avail),
            "days_open": "{}/25".format(num_days_open),
        }

        return [self.render_template(context, language_code=language_code)]
