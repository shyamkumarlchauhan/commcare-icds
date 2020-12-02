import os

from corehq.util.log import with_progress_bar
from django.core.management.base import BaseCommand

from corehq.apps.userreports.models import StaticDataSourceConfiguration
from corehq.apps.userreports.util import get_table_name
from django.db import connections
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias


class Command(BaseCommand):
    help = """
    To Dump data from dashboard tables ( Datasources, agg tables, miscs)
    """

    def add_arguments(self, parser):
        parser.add_argument('table_type', help="datasources, aggregated or misc")
        parser.add_argument('domain_name', help="domain name")
        parser.add_argument('--directory', help="Directory to put the dumps of tables")

    def handle(self, table_type, domain_name, *arg, **options):

        directory = options.get('directory', 'data_dump')
        if not os.path.exists(directory):
            os.mkdir(directory)

        tables_to_dump = []
        if table_type in ('datasources', 'all'):
            tables_to_dump += self.static_datasources_table_names(domain_name)
        elif table_type == ('aggregated', 'all'):
            pass
        elif table_type == ('misc', 'all'):
            pass

        print(f"Found Following {table_type} tables:")
        print("\n".join(tables_to_dump))
        choice = input('Do you want to create dumps (Y/N)')

        if not choice.lower().startswith('y'):
            print("No worries!!!")
            return

        tables_to_dump = sorted(tables_to_dump)
        for table_name in with_progress_bar(tables_to_dump):
            self.dump_table_data_to_csv(table_name, directory)
            print(f"Done with {table_name}")

    def static_datasources_table_names(self, domain_name):
        static_datasources = StaticDataSourceConfiguration.by_domain(domain_name)
        return [get_table_name(domain_name, datasource.table_id) for datasource in static_datasources ]

    def dump_table_data_to_csv(table_name, directory):
        query = f"""
        COPY(select * from "{table_name}")
        TO STDOUT WITH CSV HEADER DELIMITER '\t' NULL 'null'  ENCODING 'UTF-8';
        """

        file_path = f"{directory}/{table_name}.csv"
        db_cursor = connections[get_icds_ucr_citus_db_alias()].cursor()

        with open(file_path, 'wb') as fout:
            db_cursor.copy_expert(query, fout)
