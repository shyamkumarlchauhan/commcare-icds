import os
import dateutil
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand

from django.db import connections, transaction

from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias


@transaction.atomic
def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return

    with connections[db_alias].cursor() as cursor:
        cursor.execute(command)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('start_month')
        parser.add_argument('end_month')

    def handle(self, start_month, end_month, *args, **options):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', 'fix_thr_data.sql'
        )

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
            start_month += relativedelta(months=1)
