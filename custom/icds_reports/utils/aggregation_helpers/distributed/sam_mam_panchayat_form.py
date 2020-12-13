from dateutil.relativedelta import relativedelta

from custom.icds_reports.const import AGG_SAM_MAM_PANCHAYAT_TABLE
from custom.icds_reports.utils.aggregation_helpers import month_formatter
from custom.icds_reports.utils.aggregation_helpers.distributed.base import (
    StateBasedAggregationDistributedHelper,
)


class SamMamFormAggregationPanchayatDistributedHelper(StateBasedAggregationDistributedHelper):
    helper_key = 'sam-mam-form'
    ucr_data_source_id = 'static-sam_mam_visit'
    aggregate_parent_table = AGG_SAM_MAM_PANCHAYAT_TABLE
    months_required = 4

    def data_from_ucr_query(self):
        current_month_start = month_formatter(self.month)
        next_month_start = month_formatter(self.month + relativedelta(months=1))

        # We need two windows here because we want to filter the forms differently for two different
        #   columns.
        # Window definitions inspired by https://stackoverflow.com/a/47223416.
        # rank() is used because we want to find the dates of first 4 forms submitted for each case with
        #  last_visit_date and poshan_panchayat_date in the month separately.
        return """
            SELECT
                supervisor_id,
                awc_id,
                poshan_panchayat_date,
                rank() OVER (
                    PARTITION BY supervisor_id, awc_id
                    ORDER BY poshan_panchayat_date, timeend
                    ) as rank
            FROM "{ucr_tablename}" 
            WHERE 
                state_id = '{state_id}' AND
                poshan_panchayat_date >= %(current_month_start)s AND
                poshan_panchayat_date < %(next_month_start)s
        """.format(ucr_tablename=self.ucr_tablename, state_id=self.state_id), {
            "current_month_start": current_month_start,
            "next_month_start": next_month_start
        }

    def aggregation_query(self):
        month = self.month.replace(day=1)
        next_month_start = month_formatter(self.month + relativedelta(months=1))
        ucr_query, ucr_query_params = self.data_from_ucr_query()
        query_params = {
            "month": month_formatter(month),
            "previous_month": month_formatter(month - relativedelta(months=1)),
            "state_id": self.state_id,
            "next_month_start": next_month_start
        }
        query_params.update(ucr_query_params)


        return """
        INSERT INTO "{tablename}" (
            state_id,
            supervisor_id,
            month,
            awc_id,
            poshan_panchayat_date_1,
            poshan_panchayat_date_2,
            poshan_panchayat_date_3,
            poshan_panchayat_date_4
        ) (
          SELECT
            %(state_id)s AS state_id,
            supervisor_id,
            %(month)s::date AS month,
            awc_id,
            MIN(CASE WHEN rank=1 THEN poshan_panchayat_date ELSE NULL END
                ) AS poshan_panchayat_date_1,
            MIN(CASE WHEN rank=2 THEN poshan_panchayat_date ELSE NULL END
                ) AS poshan_panchayat_date_2,
            MIN(CASE WHEN rank=3 THEN poshan_panchayat_date ELSE NULL END
                ) AS poshan_panchayat_date_3,
            MIN(CASE WHEN rank=4 THEN poshan_panchayat_date ELSE NULL END
                ) AS poshan_panchayat_date_4
          FROM ({ucr_table_query}) ucr
          group by state_id, supervisor_id, month, awc_id
        )
        """.format(
            ucr_table_query=ucr_query,
            tablename=self.aggregate_parent_table,
            prev_tablename=self.prev_tablename
        ), query_params
