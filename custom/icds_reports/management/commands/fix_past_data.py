import os

import dateutil
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction


@transaction.atomic
def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return
    with connections[db_alias].cursor() as cursor:
        cursor.execute(command, [])


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('start_month')
        parser.add_argument('end_month')

    def sql_to_execute(self, month, next_month, file):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', file
        )
        with open(path, "r", encoding='utf-8') as sql_file:
            sql_query = sql_file.read()
        query = sql_query.format(month=month, next_month=next_month)
        for quer in query.split(';'):
            _run_custom_sql_script(quer)


    def handle(self, start_month, end_month, *args, **options):
        start_month = dateutil.parser.parse(start_month).date().replace(day=1)
        end_month = dateutil.parser.parse(end_month).date().replace(day=1)

        while start_month <= end_month:
            print("PROCESSING MONTH {}".format(start_month))
            next_month = start_month + relativedelta(months=1)
            self.sql_to_execute(start_month, next_month, 'data_fix.sql')
            start_month = next_month
