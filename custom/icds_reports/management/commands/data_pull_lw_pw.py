import csv
import datetime
from dateutil import parser
from dimagi.utils.chunked import chunked
from django.core.management.base import BaseCommand
from django.db import connections

from corehq.form_processor.interfaces.dbaccessors import CaseAccessors
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias


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
        "awc_location"."doc_id" AS "awc_id",
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
        MIN("agg_awc"."month") AS "month",
        '0'::int as "count"
    FROM "public"."agg_awc" "agg_awc"
    LEFT JOIN "public"."awc_location_local" "awc_location" ON (
        ("awc_location"."aggregation_level" = "agg_awc"."aggregation_level") AND
        ("awc_location"."state_id" = "agg_awc"."state_id") AND
        ("awc_location"."district_id" = "agg_awc"."district_id") AND
        ("awc_location"."block_id" = "agg_awc"."block_id") AND
        ("awc_location"."supervisor_id" = "agg_awc"."supervisor_id") AND
        ("awc_location"."doc_id" = "agg_awc"."awc_id")
    ) WHERE
    "awc_location"."aggregation_level" = 5
    AND "agg_awc"."aggregation_level" = 5
    AND "agg_awc"."state_is_test" <> 1
    GROUP BY
    "awc_location"."doc_id",
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

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------
#  GroupAggregate  (cost=2829187.69..2829187.72 rows=1 width=284)
#    Group Key: awc_location.doc_id, awc_location.supervisor_id, awc_location.block_id, awc_location.district_id, awc_location.state_id
#    ->  Sort  (cost=2829187.69..2829187.69 rows=1 width=280)
#          Sort Key: awc_location.doc_id, awc_location.supervisor_id, awc_location.block_id, awc_location.district_id, awc_location.state_id
#          ->  Gather  (cost=96090.08..2829187.68 rows=1 width=280)
#                Workers Planned: 4
#                ->  Parallel Hash Join  (cost=95090.08..2828187.58 rows=1 width=280)
#                      Hash Cond: ((agg_awc_36.state_id = awc_location.state_id) AND (agg_awc_36.district_id = awc_location.district_id) AND (agg_awc_36.block_id = awc_location.block_id) AND (agg_awc_36.supervisor_id = awc_location.supervisor_id) AND (agg_
# awc_36.awc_id = awc_location.doc_id))
#                      ->  Parallel Append  (cost=0.00..2486931.91 rows=3847969 width=173)

query_2 = """
    SELECT
        case_id, awc_id
    FROM ccs_record_monthly
    WHERE month>='2018-08-01' and month<='2020-08-01' AND (pregnant_all=1 OR lactating_all=1)
    GROUP BY case_id, awc_id;
"""


#                                                                               QUERY PLAN
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  HashAggregate  (cost=0.00..0.00 rows=0 width=0)
#    Group Key: remote_scan.case_id, remote_scan.awc_id
#    ->  Custom Scan (Citus Real-Time)  (cost=0.00..0.00 rows=0 width=0)
#          Task Count: 64
#          Tasks Shown: One of 64
#          ->  Task
#                Node: host=100.71.184.232 port=6432 dbname=icds_ucr
#                ->  Group  (cost=234168.90..385306.52 rows=297385 width=70)
#                      Group Key: case_id, awc_id
#                      ->  Gather Merge  (cost=234168.90..379352.70 rows=1190765 width=70)
#                            Workers Planned: 5
#                            ->  Group  (cost=233168.82..234954.97 rows=238153 width=70)
#                                  Group Key: case_id, awc_id
#                                  ->  Sort  (cost=233168.82..233764.20 rows=238153 width=70)
#                                        Sort Key: case_id, awc_id
#                                        ->  Parallel Seq Scan on ccs_record_monthly_102712 ccs_record_monthly  (cost=0.00..200456.84 rows=238153 width=70)
#                                              Filter: ((month >= '2018-08-01'::date) AND (month <= '2020-08-01'::date) AND ((pregnant_all = 1) OR (lactating_all = 1)))

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
        for chunk in chunked(ccs_data.keys(), 100000):
            for case in case_accessor.get_cases(list(chunk)):
                lmp = case.get_case_property('lmp')
                end_date = datetime.datetime.now()
                start_date = datetime.datetime(2018, 8, 1)
                try:
                    if lmp and start_date <=parser.parse(lmp)<= end_date:
                        awc_locs[ccs_data[case.case_id]]['count'] = awc_locs[ccs_data[case.case_id]]['count'] + 1
                except Exception as e:
                    print(e)
            print(f"{i*10000} / {total} record processed\n")
            i = i + 1
        csv_dict = list(awc_locs.values())
        csv_columns = csv_dict[0].keys()
        print("preparing file ====\n")
        with open('pw_lw_data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in csv_dict:
                writer.writerow(data)
