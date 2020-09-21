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
        parser.add_argument('month')

    def handle(self, month, *args, **options):
        path = os.path.join(
            os.path.dirname(__file__), 'sql_scripts', 'update_agg_awc_launched_nums.sql'
        )

        start_month = dateutil.parser.parse(month).date().replace(day=1)
        end_month = start_month + relativedelta(months=1, seconds=-1)
        with open(path, "r", encoding='utf-8') as sql_file:
            sql_to_execute = sql_file.read()
            split_sqls = sql_to_execute.split(";;")
            print("PROCESSING MONTH {}".format(month))
            for split_sql in split_sqls:
                query = split_sql.format(month=start_month, month_end=end_month)
                _run_custom_sql_script(query)
