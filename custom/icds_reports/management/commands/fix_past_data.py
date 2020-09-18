import os

import dateutil
from custom.icds_reports.models.aggregate import AwcLocation
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from corehq.util.argparse_types import date_type


@transaction.atomic
def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return
    with connections[db_alias].cursor() as cursor:
        cursor.execute(command, [])


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            'start_month',
            type=date_type,
            help='The start date (inclusive). format YYYY-MM-DD'
        )
        parser.add_argument(
            'end_month',
            type=date_type,
            help='The end date (inclusive). format YYYY-MM-DD'
        )
    def state_wise_query(self, state_ids, start_month, next_month, script_name):
        for state_id in state_ids:
            self.sql_to_execute(start_month, next_month, script_name, state_id)

    def sql_to_execute(self, month, next_month, file, state_id=None):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', file
        )
        with open(path, "r", encoding='utf-8') as sql_file:
            sql_query = sql_file.read()
        query = sql_query.format(month=month, next_month=next_month, state_id=state_id)
        for quer in query.split(';'):
            _run_custom_sql_script(quer)
        print(f"Executed file {file}")
        if state_id:
            print(f"Executed query {state_id}")


    def handle(self, start_month, end_month, *args, **options):
        state_ids = AwcLocation.objects.filter(aggregation_level=1).values_list('state_id', flat=True).distinct()

        while start_month <= end_month:
            print("PROCESSING MONTH {}".format(start_month))
            next_month = start_month + relativedelta(months=1)
            # state wise queries
            self.sql_to_execute(start_month, next_month, 'fix_pse_data_3.sql')
            self.state_wise_query(state_ids, start_month, next_month, 'fix_pse_data_2.sql')
            # normal queries
            self.sql_to_execute(start_month, next_month, 'fix_pse_data_1.sql')
            start_month = next_month
