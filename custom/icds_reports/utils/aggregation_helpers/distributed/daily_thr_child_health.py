from dateutil.relativedelta import relativedelta

from custom.icds_reports.const import AGG_DAILY_CHILD_HEALTH_THR_TABLE
from custom.icds_reports.utils.aggregation_helpers import month_formatter, transform_day_to_month
from custom.icds_reports.utils.aggregation_helpers.distributed.base import BaseICDSAggregationDistributedHelper


class DailyTHRChildHealthHelper(BaseICDSAggregationDistributedHelper):
    helper_key = 'daily-thr-child-health'
    ucr_data_source_id = 'static-dashboard_thr_forms'
    tablename = AGG_DAILY_CHILD_HEALTH_THR_TABLE

    def __init__(self, month):
        self.month = transform_day_to_month(month)
        self.next_month_start = self.month + relativedelta(months=1)
        self.current_month_start = self.month

    @property
    def monthly_tablename(self):
        return f"{self.tablename}_{month_formatter(self.month)}"

    def aggregate(self, cursor):
        create_table_query = self.create_table_query()
        drop_table_query = self.drop_table_query()
        agg_query, agg_params = self.aggregation_query()
        index_queries = self.indexes()
        add_partition_query = self.add_partition_table__query()

        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        cursor.execute(agg_query, agg_params)

        for query in index_queries:
            cursor.execute(query)

        cursor.execute(add_partition_query)

    def drop_table_query(self):
        return f"""
                DROP TABLE IF EXISTS "{self.monthly_tablename}"
            """

    def create_table_query(self):
        return f"""
            CREATE TABLE "{self.monthly_tablename}" (LIKE {self.tablename});
            SELECT create_distributed_table('{self.monthly_tablename}', 'supervisor_id');
        """

    def aggregation_query(self):
        query_params = {
            "month": self.current_month_start,
            "current_month_start": self.current_month_start,
            "next_month_start": self.next_month_start,
        }

        return """
                INSERT INTO "{monthly_tablename}" (
                  doc_id, state_id, supervisor_id, month, case_id, latest_time_end_processed,
                  photo_thr_packets_distributed
                ) (
                  SELECT
                    doc_id,
                    state_id,
                    supervisor_id,
                    %(month)s AS month,
                    child_health_case_id as case_id,
                    timeend AS latest_time_end_processed,
                    photo_thr_packets_distributed
                  FROM "{ucr_tablename}"
                  WHERE timeend >= %(current_month_start)s AND timeend < %(next_month_start)s AND
                        child_health_case_id IS NOT NULL AND days_ration_given_child IS NOT NULL
                )
                """.format(
            ucr_tablename=self.ucr_tablename,
            monthly_tablename=self.monthly_tablename,
        ), query_params

    def indexes(self):
        return [
            f"""CREATE INDEX IF NOT EXISTS thr_child_health_daily_idx
                ON "{self.tablename}" (month, state_id, case_id, supervisor_id)
            """
        ]

    def add_partition_table__query(self):
        return f"""
            ALTER TABLE "{self.tablename}" ATTACH PARTITION "{self.monthly_tablename}"
            FOR VALUES IN ('{month_formatter(self.month)}')
        """
