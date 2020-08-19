import csv
import datetime
from dateutil import parser
from dimagi.utils.chunked import chunked
from django.core.management.base import BaseCommand
from django.db import connections

from corehq.form_processor.interfaces.dbaccessors import CaseAccessors
from corehq.sql_db.connections import get_icds_ucr_citus_db_alias


def _run_custom_sql_script(command):
    db_alias = get_icds_ucr_citus_db_alias()
    if not db_alias:
        return []
    with connections[db_alias].cursor() as cursor:
        cursor.execute(command)
        row = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
        return row


query_1 = """
    SELECT
        "awc_location"."awc_id" AS "awc_id",
        "awc_location"."awc_name" AS "awc_name",
        "awc_location"."awc_site_code" AS "awc_site_code",
        "awc_location"."supervisor_id" AS "supervisor_id",
        "awc_location"."supervisor_name" AS "supervisor_name",
        "awc_location"."supervisor_site_code" AS "supervisor_site_code",
        "awc_location"."block_id" AS "block_id",
        "awc_location"."block_name" AS "block_name",
        "awc_location"."block_site_code" AS "block_site_code",
        "awc_location"."district_id" AS "district_id",
        "awc_location"."district_name" AS "district_name",
        "awc_location"."district_site_code" AS "district_site_code",
        "awc_location"."state_id" AS "state_id",
        "awc_location"."state_name" AS "state_name",
        "awc_location"."state_site_code" AS "state_site_code",
        "awc_location"."aggregation_level" AS "aggregation_level",
        MIN("awc_location"."month") AS "month",
        '0'::int as "count"
    FROM "public"."agg_awc" "agg_awc"
    LEFT JOIN "public"."awc_location_local" "awc_location" ON (
        ("awc_location"."month" = "agg_awc"."month") AND
        ("awc_location"."aggregation_level" = "agg_awc"."aggregation_level") AND
        ("awc_location"."state_id" = "agg_awc"."state_id") AND
        ("awc_location"."district_id" = "agg_awc"."district_id") AND
        ("awc_location"."block_id" = "agg_awc"."block_id") AND
        ("awc_location"."supervisor_id" = "agg_awc"."supervisor_id") AND
        ("awc_location"."awc_id" = "agg_awc"."awc_id")
    ) WHERE
    "awc_location"."aggregation_level" = 5
    AND "agg_awc"."aggregation_level" = 5
    AND "agg_awc"."state_is_test" <> 1
    GROUP BY
    "awc_location"."awc_id",
    "awc_location"."awc_name",
    "awc_location"."awc_site_code",
    "awc_location"."supervisor_id",
    "awc_location"."supervisor_name",
    "awc_location"."supervisor_site_code",
    "awc_location"."block_id",
    "awc_location"."block_name",
    "awc_location"."block_site_code",
    "awc_location"."district_id",
    "awc_location"."district_name",
    "awc_location"."district_site_code",
    "awc_location"."state_id",
    "awc_location"."state_name",
    "awc_location"."state_site_code",
    "awc_location"."aggregation_level";
"""

query_2 = """
    SELECT
        case_id, awc_id
    FROM ccs_Record_monthly
    WHERE month>='2018-08-01' and month<='2020-08-01' AND (pregnant_all=1 OR lactating_all=1)
    GROUP BY case_id, awc_id;
"""


class Command(BaseCommand):
    help = "Run Custom Data Pull"

    def handle(self, **options):
        case_accessor = CaseAccessors('icds-cas')
        awc_locs = {item['awc_id']: item for item in _run_custom_sql_script(query_1)}
        print("locations fetched========\n")
        ccs_data = {item['case_id']: item['awc_id'] for item in _run_custom_sql_script(query_2)}
        print("ccs records fetched======\n")
        i = 0
        total = len(ccs_data)
        for chunk in chunked(ccs_data.keys(), 10000):
            for case in case_accessor.get_cases(list(chunk)):
                lmp = case.get_case_property('lmp')
                end_date = datetime.datetime(2020, 8, 1)
                start_date = datetime.datetime(2018, 8, 1)
                try:
                    if lmp is not None and lmp != '' and parser.parse(lmp) <= end_date and parser.parse(
                        lmp) >= start_date:
                        awc_locs[ccs_data[case.case_id]]['count'] = awc_locs[ccs_data[case.case_id]]['count'] + 1
                except Exception as e:
                    print(e)
            print(f"{i*10000} / {total} record processed\n")
            i = i + 1
        csv_dict = list(awc_locs.values())
        csv_columns = awc_locs[0].keys()
        print("preparing file ====\n")
        with open('pw_lw_data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in csv_dict:
                writer.writerow(data)
