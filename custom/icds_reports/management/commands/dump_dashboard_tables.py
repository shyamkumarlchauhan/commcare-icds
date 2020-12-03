import os

from corehq.util.log import with_progress_bar
from django.core.management.base import BaseCommand

from corehq.apps.userreports.models import StaticDataSourceConfiguration
from corehq.apps.userreports.util import get_table_name
from django.db import connections
from custom.icds_reports.utils.connections import get_icds_ucr_citus_db_alias
from custom.icds_reports.models.aggregate import AwcLocation


class Command(BaseCommand):
    help = """
    To Dump data from dashboard tables ( Datasources, agg tables, miscs)
    """

    state_id = None
    tables_with_special_filter = {
        'static-ag_care_cases_monthly': 'supervisor_id',
        'static-home_visit_forms': 'supervisor_id',
        'static-usage_forms': 'supervisor_id',
        'static-it_report_follow_issue': 'user_location_id',
        'static-commcare_user_cases': 'commcare_location_id'
    }

    def add_arguments(self, parser):
        parser.add_argument('table_type', help="datasources, aggregated or misc")
        parser.add_argument('state_id')
        parser.add_argument('domain_name', help="domain name")
        parser.add_argument('--directory', help="Directory to put the dumps of tables")

    def handle(self, table_type, state_id, domain_name, *arg, **options):

        directory = options.get('directory', 'data_dump')
        if not os.path.exists(directory):
            os.mkdir(directory)

        self.state_id = state_id
        self.all_sub_location_ids = self.get_all_sub_locations()

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
        for table_name, filter_column in tables_to_dump:
            self.dump_table_data_to_csv(table_name,filter_column, directory)
            print(f"Done with {table_name}")

    def static_datasources_table_names(self, domain_name):
        static_datasources = StaticDataSourceConfiguration.by_domain(domain_name)

        return [
            (
                get_table_name(domain_name, datasource.table_id),
                self.tables_special_filter.get(datasource.table_id,'state_id')
             ) for datasource in static_datasources
        ]

    def dump_table_data_to_csv(self, table_name, filter_column, directory):
        def get_dump_query(filter_column):
            if filter_column == 'state_id':
                return f"""
                COPY(select * from "{table_name}" where state_id='{self.state_id}')
                TO STDOUT WITH CSV HEADER DELIMITER '\t' NULL 'null'  ENCODING 'UTF-8';
                """
            elif filter_column == 'supervisor_id':
                supervisor_ids = {row['supervisor_id'] for row in self.all_sub_location_ids}
                supervisor_ids_filters = "','".join(supervisor_ids)
                return f"""
                COPY(select * from "{table_name}" where supervisor_id in ('{supervisor_ids_filters}'))
                TO STDOUT WITH CSV HEADER DELIMITER '\t' NULL 'null'  ENCODING 'UTF-8';
                """
            elif filter_column == 'user_location_id':
                block_ids = {row['block_ids'] for row in self.all_sub_location_ids}
                block_ids_filter = "','".join(block_ids)
                return f"""
                COPY(select * from "{table_name}" where user_location_id in ('{block_ids_filter}'))
                TO STDOUT WITH CSV HEADER DELIMITER '\t' NULL 'null'  ENCODING 'UTF-8';
                """
            elif filter_column == 'commcare_location_id':

                all_sub_location_ids = set()
                for row in self.all_sub_location_ids:
                    all_sub_location_ids.add(row['block_id'])
                    all_sub_location_ids.add(row['supervisor_id'])
                    all_sub_location_ids.add(row['awc_id'])

                location_filter = "','".join(all_sub_location_ids)
                return f"""
                        COPY(select * from "{table_name}" where commcare_location_id in ('{location_filter}'))
                        TO STDOUT WITH CSV HEADER DELIMITER '\t' NULL 'null'  ENCODING 'UTF-8';
                        """
            return None

        query = get_dump_query(filter_column)
        if not query:
            print(f"skipping table {table_name}")
            return

        file_path = f"{directory}/{table_name}.csv"

        with connections[get_icds_ucr_citus_db_alias()].cursor() as db_cursor:
            with open(file_path, 'wb') as fout:
                db_cursor.copy_expert(query, fout)

    def get_all_sub_locations(self):
        query_set = AwcLocation.objects.filter(aggregation_level=5,
                                               state_id=self.state_id).values('doc_id',
                                                                              'supervisor_id',
                                                                              'block_id').order_by('doc_id')
        return list(query_set)
