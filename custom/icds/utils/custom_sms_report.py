import json

from dimagi.utils.couch.cache.cache_core import get_redis_client

from custom.icds.const import MAX_CONCURRENT_SMS_REPORTS_ALLOWED


class CustomSMSReportTracker(object):
    def __init__(self, domain):
        self.client = get_redis_client()
        self.domain = domain

    @property
    def report_key(self):
        return '{domain}-custom-sms-report-tracker'.format(domain=self.domain)

    @property
    def key_expiry(self):
        return 48 * 60 * 60

    def add_report(self, start_date: str, end_date: str):
        reports = self.active_reports
        reports.append('{start_date}--{end_date}'.format(
            start_date=start_date,
            end_date=end_date,
        ))
        self.save_reports_info(reports)

    @property
    def active_reports(self):
        reports = []
        serialized_report_list = self.client.get(self.report_key)
        if(serialized_report_list):
            reports = json.loads(serialized_report_list)
        return reports

    def save_reports_info(self, reports):
        self.client.set(self.report_key, json.dumps(reports))
        self.client.expire(self.report_key, self.key_expiry)

    def remove_report(self, start_date: str, end_date: str):
        reports = self.active_reports
        report_id = _get_report_id(start_date, end_date)
        retain_reports = [report for report in reports
                    if report != report_id]
        if len(retain_reports) == 0:
            self.clear_all_reports()
        else:
            self.save_reports_info(retain_reports)

    def clear_all_reports(self):
        self.client.delete(self.report_key)


def _get_report_id(start_date: str, end_date: str):
    return start_date + '--' + end_date


def _can_add_new_report(report_count):
    return report_count < MAX_CONCURRENT_SMS_REPORTS_ALLOWED
