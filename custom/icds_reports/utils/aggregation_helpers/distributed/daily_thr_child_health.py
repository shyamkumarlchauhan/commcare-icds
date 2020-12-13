from dateutil.relativedelta import relativedelta

from custom.icds_reports.const import AGG_DAILY_CHILD_HEALTH_THR_TABLE
from custom.icds_reports.utils.aggregation_helpers import month_formatter, transform_day_to_month
from custom.icds_reports.utils.aggregation_helpers.distributed.base import StateBasedAggregationDistributedHelper


class DailyTHRChildHealthHelper(StateBasedAggregationDistributedHelper):
    helper_key = 'daily-thr-child-health'
    ucr_data_source_id = 'static-dashboard_thr_forms'
    aggregate_parent_table = AGG_DAILY_CHILD_HEALTH_THR_TABLE

    def __init__(self, state_id, month):
        self.state_id = state_id
        self.month = transform_day_to_month(month)
        self.next_month_start = self.month + relativedelta(months=1)
        self.current_month_start = self.month

    def aggregation_query(self):
        query_params = {
            "month": self.current_month_start,
            "state_id": self.state_id,
            "current_month_start": self.current_month_start,
            "next_month_start": self.next_month_start,
        }

        return """
                INSERT INTO "{tablename}" (
                  doc_id, state_id, supervisor_id, month, case_id, latest_time_end_processed,
                  photo_thr_packets_distributed
                ) (
                  SELECT
                    doc_id,
                    %(state_id)s AS state_id,
                    supervisor_id,
                    %(month)s AS month,
                    child_health_case_id as case_id,
                    timeend AS latest_time_end_processed,
                    photo_thr_packets_distributed
                  FROM "{ucr_tablename}"
                  WHERE state_id = %(state_id)s AND
                        timeend >= %(current_month_start)s AND timeend < %(next_month_start)s AND
                        child_health_case_id IS NOT NULL AND days_ration_given_child IS NOT NULL
                )
                """.format(
            ucr_tablename=self.ucr_tablename,
            tablename=self.aggregate_parent_table,
        ), query_params
