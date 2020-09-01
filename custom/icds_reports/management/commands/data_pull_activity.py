import csv
import datetime
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from dateutil import parser
from dimagi.utils.chunked import chunked
from django.core.management.base import BaseCommand
from django.db import connections

from corehq.form_processor.interfaces.dbaccessors import CaseAccessors


def _run_custom_sql_script(command, data=True):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return []
    with connections[db_alias].cursor() as cursor:
        cursor.execute(command)
        if data:
            row = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
            return row


query_0 = """
    CREATE TABLE tmp_usage AS SELECT awc_id, count(*) as usage_count
            FROM "ucr_icds-cas_static-usage_forms_92fbe2aa" WHERE
        month='{month}'
        GROUP BY awc_id
"""

query_1 = """
    SELECT
        "awc_location"."awc_name" AS "awc_name",
        "awc_location"."awc_name" AS "awc_site_code",
        "awc_location"."district_id" AS "district_site_code",
        "awc_location"."district_name" AS "district_name",
        "awc_location"."state_id" AS "state_site_code",
        "awc_location"."state_name" AS "state_name",
        "awc_location"."state_id" AS "block_site_code",
        "awc_location"."state_name" AS "block_name",
        "awc_location"."state_id" AS "district_site_code",
        "awc_location"."state_name" AS "district_name",
        "agg_awc"."month" AS "month",
        usage_form.awc_id as awc_id
    FROM awc_location_local "awc_location"
    LEFT JOIN "public"."agg_awc" "agg_awc" ON (
        ("awc_location"."aggregation_level" = "agg_awc"."aggregation_level") AND
        ("awc_location"."state_id" = "agg_awc"."state_id") AND
        ("awc_location"."district_id" = "agg_awc"."district_id") AND
        ("awc_location"."supervisor_id" = "agg_awc"."supervisor_id") AND
        ("awc_location"."doc_id" = "agg_awc"."awc_id")
    ) LEFT JOIN "tmp_usage" usage_form ON (
        (usage_form.awc_id = "awc_location"."doc_id")
    ) WHERE "awc_location".aggregation_level=5 AND
    "agg_awc".aggregation_level=5 AND
    "agg_awc"."month" = '{month}' AND
    "agg_awc".num_launched_awcs>0 AND
    usage_form.awc_id IS NULL AND
    usage_form.month='{month}' AND
    "awc_location"."state_id" = '{state_id}'
"""

query_1 = """
    DROP TABLE IF EXISTS tmp_usage
"""
STATE_ID = 'f98e91aa003accb7b849a0f18ebd7039'

class Command(BaseCommand):
    help = "Run Custom Data Pull"

    def execute_data_pull(self, start_date):
        print("===Creating temp tables===")
        _run_custom_sql_script(query_0.format(month=start_date), False)
        print("===fetching data====")
        csv_dict = _run_custom_sql_script(query_1.format(month=start_date, state_id=STATE_ID))
        print("===preparing file====")
        csv_columns = csv_dict[0].keys()
        with open(f'activity_report_{start_date}.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in csv_dict:
                writer.writerow(data)
        print("===dropping tmp table===")
        _run_custom_sql_script(query_1.format(month=start_date), False)

    def handle(self, **options):
        start_date = date(2019, 6, 1)
        end_date = date(2020, 6, 1)
        while start_date <= end_date:
            print(f"Running data pull for {start_date}")
            self.execute_data_pull(start_date)
            start_date = start_date + relativedelta(months=1)
