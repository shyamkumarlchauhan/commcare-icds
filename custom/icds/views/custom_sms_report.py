from django.contrib import messages
from django.utils.decorators import method_decorator
from django.utils.translation import ugettext_lazy as _

from memoized import memoized

from corehq.apps.hqwebapp.decorators import use_daterangepicker
from corehq.apps.sms.views import BaseMessagingSectionView
from custom.icds import icds_toggles
from custom.icds.const import MAX_CONCURRENT_SMS_REPORTS_ALLOWED
from custom.icds.forms import CustomSMSReportRequestForm
from custom.icds.tasks.sms import send_custom_sms_report
from custom.icds.utils.custom_sms_report import (
    CustomSMSReportTracker,
    _get_report_id,
    _can_add_new_report,
)
from custom.icds_core.const import SMSUsageReport_urlname


@method_decorator(icds_toggles.ICDS_CUSTOM_SMS_REPORT.required_decorator(), name='dispatch')
class SMSUsageReport(BaseMessagingSectionView):
    template_name = 'icds/sms/custom_sms_report.html'
    urlname = SMSUsageReport_urlname
    page_title = _('Custom SMS Usage Report')

    @use_daterangepicker
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    @property
    def page_context(self):
        report_ids = self._get_active_report_ids()
        report_count = len(report_ids)
        context = {}
        disable_submit = not _can_add_new_report(report_count)
        if disable_submit:
            messages.error(self.request, _('Only {max_report_count} concurrent reports are allowed at a time.\
                Please wait for them to finish.').format(
                max_report_count=MAX_CONCURRENT_SMS_REPORTS_ALLOWED
            ))
        if report_count > 0:
            context['reports_in_progress'] = ',  '.join(report_ids)

        self.request_form = CustomSMSReportRequestForm(disable_submit=disable_submit)
        context['request_form'] = self.request_form
        return context

    @memoized
    def _get_active_report_ids(self):
        report_tracker = CustomSMSReportTracker(self.request.domain)
        return report_tracker.active_reports

    def post(self, request, *args, **kwargs):
        report_tracker = CustomSMSReportTracker(request.domain)
        reports_in_progress = report_tracker.active_reports
        report_count = len(reports_in_progress)

        self.request_form = CustomSMSReportRequestForm(request.POST)

        if self.request_form.is_valid() and _can_add_new_report(report_count):
            user_email = self.request.user.email
            data = self.request_form.cleaned_data
            start_date = data['start_date']
            end_date = data['end_date']
            if self.set_error_messages(reports_in_progress, start_date, end_date):
                return self.get(*args, **kwargs)

            report_tracker.add_report(str(start_date), str(end_date))

            send_custom_sms_report.delay(str(start_date), str(end_date), user_email, self.request.domain)

            messages.success(self.request, _(
                "Report will we soon emailed to your email i.e {user_email}"
                .format(user_email=user_email))
            )
        else:
            for message_list in self.request_form.errors.values():
                for message in message_list:
                    messages.error(self.request, message)
        return self.get(*args, **kwargs)

    def set_error_messages(self, reports, start_date, end_date):
        has_errors = False
        if not self.request.user.email:
            messages.error(self.request, _("Unable to find any email associated with your account"))
            has_errors = True
        if _report_already_in_progress(reports, str(start_date), str(end_date)):
            messages.error(
                self.request,
                _("Report for duration {start_date}-{end_date} already in progress").format(
                    start_date=start_date,
                    end_date=end_date,
                ))
            has_errors = True
        return has_errors


def _report_already_in_progress(reports, start_date: str, end_date: str):
    report_id = _get_report_id(start_date, end_date)
    return any(report == report_id for report in reports)
