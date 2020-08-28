import os
import dateutil
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand

from django.db import connections, transaction

from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from custom.icds_reports.models.aggregate import AwcLocation


@transaction.atomic
def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return

    with connections[db_alias].cursor() as cursor:
        cursor.execute(command)


query_state_wise = """
    DROP TABLE IF EXISTS temp_agg_child_health_rhit;
    CREATE UNLOGGED TABLE temp_agg_child_health_rhit AS (select state_id,supervisor_id,awc_id,month,gender,age_tranche,caste,disabled,minority,resident, rations_21_plus_distributed, days_ration_given_child from agg_child_health where 1=0);
    SELECT create_distributed_table('temp_agg_child_health_rhit', 'supervisor_id');
    INSERT INTO temp_agg_child_health_rhit(
            state_id,
            supervisor_id,
            awc_id,
            month,
            gender,
            age_tranche,
            caste,
            disabled,
            minority,
            resident,
            rations_21_plus_distributed,
            days_ration_given_child
    )
    (
        select
            state_id,
            supervisor_id,
            awc_id,
            month,
            sex,
            age_tranche,
            caste,
            COALESCE(chm.disabled, 'no') as coalesce_disabled,
            COALESCE(chm.minority, 'no') as coalesce_minority,
            COALESCE(chm.resident, 'no') as coalesce_resident,
            SUM(CASE WHEN chm.num_rations_distributed >= 21 THEN 1 ELSE 0 END) as rations_21_plus_distributed,
            SUM(chm.days_ration_given_child) as days_ration_given_child
        from child_health_monthly chm
            WHERE month = '{month}' AND state_id='{state_id}'
        group by  state_id,
            supervisor_id,
            awc_id,
            month,
            sex,
            age_tranche,
            caste,
            coalesce_disabled,
            coalesce_minority,
            coalesce_resident
    );
    
    
    DROP TABLE IF EXISTS temp_agg_child;
    CREATE UNLOGGED TABLE temp_agg_child AS (select * from temp_agg_child_health_rhit);
    UPDATE agg_child_health
        SET
            rations_21_plus_distributed = thr_temp.rations_21_plus_distributed,
            days_ration_given_child = thr_temp.days_ration_given_child
    
    from temp_agg_child thr_temp
    where
        agg_child_health.state_id = thr_temp.state_id AND
        agg_child_health.supervisor_id = thr_temp.supervisor_id AND
        agg_child_health.awc_id = thr_temp.awc_id AND
        agg_child_health.month = thr_temp.month AND
        agg_child_health.gender = thr_temp.gender AND
        agg_child_health.age_tranche = thr_temp.age_tranche AND
        agg_child_health.caste = thr_temp.caste AND
        agg_child_health.disabled = thr_temp.disabled AND
        agg_child_health.minority = thr_temp.minority AND
        agg_child_health.resident = thr_temp.resident AND
        agg_child_health.aggregation_level = 5 AND
        agg_child_health.month='{month}' AND
        agg_child_health.state_id='{state_id}'
"""


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('start_month')
        parser.add_argument('end_month')


    def state_wise_query(self, state_ids, start_month):
        for state_id in state_ids:
            query = query_state_wise.format(month=start_month, state_id=state_id)
            for quer in query.split(';'):
                _run_custom_sql_script(quer)
            print(f"Executing query {state_id}")


    def handle(self, start_month, end_month, *args, **options):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', 'fix_thr_data.sql'
        )
        state_ids = AwcLocation.objects.filter(aggregation_level=1).values_list('state_id', flat=True).distinct()

        start_month = dateutil.parser.parse(start_month).date().replace(day=1)
        end_month = dateutil.parser.parse(end_month).date().replace(day=1)
        with open(path, "r", encoding='utf-8') as sql_file:
            sql_to_execute = sql_file.read()

        while start_month <= end_month:
            print("PROCESSING MONTH {}".format(start_month))
            next_month = start_month + relativedelta(months=1)
            query = sql_to_execute.format(month=start_month, next_month=next_month)
            count = 1
            for quer in query.split(';'):
                print(f"Executing query {count}")
                _run_custom_sql_script(quer)
                count = count + 1
                if count == 2:
                    self.state_wise_query(state_ids, start_month)
            start_month += relativedelta(months=1)
