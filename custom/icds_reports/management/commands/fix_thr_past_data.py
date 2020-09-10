import dateutil
import os
from custom.icds_reports.models.aggregate import AwcLocation
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from custom.icds_reports.tasks import update_service_delivery_report
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction


@transaction.atomic
def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return
    with connections[db_alias].cursor() as cursor:
        cursor.execute(command)


initial_query = """
    UPDATE child_health_monthly
    set num_rations_distributed = CASE WHEN thr_eligible=1 THEN COALESCE(thr.days_ration_given_child, 0) ELSE NULL END,
        days_ration_given_child = thr.days_ration_given_child
FROM (
    SELECT DISTINCT ON (child_health_case_id)
            LAST_VALUE(supervisor_id) over w AS supervisor_id,
            '{month}'::date AS month,
            child_health_case_id AS case_id,
            MAX(timeend) over w AS latest_time_end_processed,
            CASE WHEN SUM(days_ration_given_child) over w > 32767 THEN 32767 ELSE SUM(days_ration_given_child) over w END AS days_ration_given_child
          FROM "ucr_icds-cas_static-dashboard_thr_forms_b8bca6ea"
          WHERE state_id = {state_id} AND timeend >= '{month}' AND timeend < '{next_month}' AND
                child_health_case_id IS NOT NULL
    WINDOW w AS (PARTITION BY supervisor_id, child_health_case_id)
    ) thr
    where
    child_health_monthly.month = thr.month AND
    child_health_monthly.case_id = thr.case_id AND
    child_health_monthly.supervisor_id = thr.supervisor_id AND
    child_health_monthly.month = '{month}';
"""


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('start_month')
        parser.add_argument('end_month')

    def state_wise_query(self, state_ids, start_month, next_month, script_name):
        for state_id in state_ids:
            query = self.sql_to_execute(start_month, next_month, script_name, state_id)
            for quer in query.split(';'):
                _run_custom_sql_script(quer)
            print(f"Executed query {state_id}")

    def sql_to_execute(self, month, next_month, file, state_id=None):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', file
        )
        with open(path, "r", encoding='utf-8') as sql_file:
            sql_query = sql_file.read()
        return sql_query.format(month=month, next_month=next_month, state_id=state_id)

    def handle(self, start_month, end_month, *args, **options):
        state_ids = AwcLocation.objects.filter(aggregation_level=1).values_list('state_id', flat=True).distinct()
        start_month = dateutil.parser.parse(start_month).date().replace(day=1)
        end_month = dateutil.parser.parse(end_month).date().replace(day=1)

        while start_month <= end_month:
            print("PROCESSING MONTH {}".format(start_month))
            next_month = start_month + relativedelta(months=1)
            # state wise queries
            self.state_wise_query(state_ids, start_month, next_month, 'fix_thr_data_3.sql')
            self.state_wise_query(state_ids, start_month, next_month, 'fix_thr_data_2.sql')
            # normal queries
            query = self.sql_to_execute(start_month, next_month, 'fix_thr_data_1.sql')
            # fix sdr
            update_service_delivery_report(start_month)
            for quer in query.split(';'):
                _run_custom_sql_script(quer)
            start_month += relativedelta(months=1)
