import csv

from django.core.management.base import BaseCommand
from django.db import connections
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias


QUERY_FOR_DUPLICATES = """SELECT 
            phone_number, 
            COUNT(*) 
        FROM 
            "ucr_icds-cas_static-person_cases_v3_2ae0879a" person_cases 
        WHERE
            person_cases.phone_number SIMILAR TO '[6789][0-9]{9}' 
            and  person_cases.migration_status=0
            and person_cases.doc_id is not null
            and person_cases.closed_on is null
            and person_cases.registered_status=1
        GROUP BY phone_number 
        HAVING COUNT(*) > 1"""

CSV_FILENAME = "duplicate_numbers.csv"


class Command(BaseCommand):
    def create_csv_file(self, rows):
        headers = ['Phone Number', 'Count']
        with open(CSV_FILENAME, 'w') as output:
            writer = csv.writer(output)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)


    def handle(self, *args, **options):
        db_alias = get_icds_ucr_citus_db_alias()
        with connections[db_alias].cursor() as cursor:
            cursor.execute(QUERY_FOR_DUPLICATES)
            output = cursor.fetchall()
            self.create_csv_file(output)