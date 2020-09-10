from dateutil.relativedelta import relativedelta

from custom.icds_reports.const import AGG_DAILY_CCS_RECORD_THR_TABLE
from custom.icds_reports.utils.aggregation_helpers import month_formatter, transform_day_to_month
from custom.icds_reports.utils.aggregation_helpers.distributed.base import StateBasedAggregationDistributedHelper


class DailyTHRCCSRecordHelper(StateBasedAggregationDistributedHelper):
    helper_key = 'daily-thr-ccs-record'
    ucr_data_source_id = 'static-dashboard_thr_forms'
    tablename = AGG_DAILY_CCS_RECORD_THR_TABLE
    temporary_tablename = 'temp_daily_thr_ccs_record'

    def __init__(self, state_id, month):
        self.state_id = state_id
        self.month = transform_day_to_month(month)
        self.next_month_start = self.month + relativedelta(months=1)
        self.current_month_start = self.month

    def aggregate(self, cursor):
        create_temp_query = self.create_temporary_table()
        drop_temp_query = self.drop_temporary_table()
        agg_query, agg_params = self.aggregation_query()
        index_queries = self.indexes()

        cursor.execute(drop_temp_query)
        cursor.execute(create_temp_query)
        cursor.execute(agg_query, agg_params)

        for query in index_queries:
            cursor.execute(query)

        for i, query in enumerate(self.aggregation_queries()):
            cursor.execute(query)
        cursor.execute(drop_temp_query)

    def create_temporary_table(self):
        return f"""
                    CREATE UNLOGGED TABLE "{self.temporary_tablename}" (LIKE {self.tablename});
                    SELECT create_distributed_table('{self.temporary_tablename}', 'supervisor_id');
                """

    def drop_temporary_table(self):
        return f"""
                    DROP TABLE IF EXISTS "{self.temporary_tablename}";
                """

    def aggregation_query(self):
        query_params = {
            "month": self.current_month_start,
            "state_id": self.state_id,
            "current_month_start": self.current_month_start,
            "next_month_start": self.next_month_start,
        }

        return """
                INSERT INTO "{temp_tablename}" (
                  doc_id, state_id, supervisor_id, month, case_id, latest_time_end_processed,
                  photo_thr_packets_distributed
                ) (
                  SELECT
                    doc_id,
                    %(state_id)s AS state_id,
                    supervisor_id,
                    %(month)s AS month,
                    ccs_record_case_id as case_id,
                    timeend AS latest_time_end_processed,
                    photo_thr_packets_distributed
                  FROM "{ucr_tablename}"
                  WHERE state_id = %(state_id)s AND
                        timeend >= %(current_month_start)s AND timeend < %(next_month_start)s AND
                        ccs_record_case_id IS NOT NULL AND days_ration_given_mother IS NOT NULL
                )
                """.format(
            ucr_tablename=self.ucr_tablename,
            temp_tablename=self.temporary_tablename,
        ), query_params

    def indexes(self):
        return [
            f"""CREATE INDEX IF NOT EXISTS thr_ccs_record_daily_idx
                ON "{self.tablename}" (month, state_id, case_id, supervisor_id)
            """
        ]

    def aggregation_queries(self):
        return [
            """DELETE FROM "{tablename}" WHERE month = '{current_month}' AND state_id = '{state_id}';""".format(
                tablename=self.tablename,
                current_month=self.month,
                state_id=self.state_id
            ),
            """INSERT INTO "{tablename}" SELECT * from "{temporary_tablename}";""".format(
                tablename=self.tablename,
                temporary_tablename=self.temporary_tablename
            ),
            """DROP TABLE "{temporary_tablename}";""".format(
                temporary_tablename=self.temporary_tablename
            )
        ]
