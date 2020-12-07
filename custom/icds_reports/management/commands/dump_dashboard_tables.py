import hashlib
import os

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
    agg_tables_not_partitioned = {
        'awc_location': 'state_id',
        'awc_location_local': 'state_id',
        'ccs_record_monthly': 'state_id',
        'icds_dashboard_comp_feed_form': 'state_id',
        'icds_dashboard_ccs_record_cf_forms': 'state_id',
        'icds_dashboard_ccs_record_postnatal_forms': 'state_id',
        'icds_dashboard_child_health_postnatal_forms': 'state_id',
        'icds_dashboard_child_health_thr_forms': 'state_id',
        'icds_dashboard_ccs_record_thr_forms': 'state_id',
        'icds_dashboard_ccs_record_bp_forms': 'state_id',
        'icds_dashboard_ccs_record_delivery_forms': 'state_id',
        'icds_dashboard_daily_child_health_thr_forms': 'state_id',
        'icds_dashboard_daily_ccs_record_thr_forms': 'state_id',
        'icds_dashboard_daily_feeding_forms': 'state_id',
        'icds_dashboard_growth_monitoring_forms': 'state_id',
        'icds_dashboard_sam_mam_forms': 'state_id',
        'icds_dashboard_thr_v2': 'state_id',
        'icds_dashboard_adolescent_girls_registration': 'state_id',
        'icds_dashboard_migration_forms': 'state_id',
        'icds_dashboard_availing_service_forms': 'state_id',
        'daily_attendance': 'state_id',
    }

    agg_tables_month_partitioned = [
        'agg_awc',
        'agg_child_health',
        'agg_ccs_record',
        'agg_service_delivery_report',
        'agg_awc_daily',
        'agg_ls',
        'child_health_monthly',
        'icds_dashboard_user_activity',

    ]
    agg_tables_state_month_partitioned = [
        ('icds_dashboard_infrastructure_forms', 'icds_db_infra_form_'),
        ('icds_dashboard_ls_awc_visits_forms', 'icds_db_ls_awc_mgt_form_'),
        ('icds_dashboard_ls_vhnd_forms', 'icds_db_ls_vhnd_form_'),
        ('icds_dashboard_ls_beneficiary_forms','icds_db_ls_beneficiary_form_')

    ]

    misc_tables = [
        'icds_months',
        'icds_months_local',
        'icds_reports_ucrreconciliationstatus',
        'icds_reports_aggregatesqlprofile',
        'icds_reports_aggregationrecord',

    ]

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
            tables_to_dump += [(table, filter_col) for table,filter_col in self.agg_tables_not_partitioned.items()]
            tables_to_dump += [ (table, 'state_id') for table in self.get_agg_tables_partitioned()]
            tables_to_dump += [(table, 'state_id') for table in self.get_agg_tables_state_partitioned()]
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


    def get_agg_tables_partitioned(self):
        # Query to find all partitioned attached to a table
        query_tamplate = """
        SELECT relname
        FROM pg_class,pg_inherits
        WHERE pg_class.oid=pg_inherits.inhrelid AND inhparent IN (SELECT oid FROM pg_class WHERE relname='{}')
        ORDER BY relname;
        """

        # Shell command to dump the schema of tables
        dump_query_template = """
        sudo -u postgres pg_dump -d icds_ucr -s {child_tables} > {parent_table}.sql
        """

        all_agg_child_tables = []
        for table in self.agg_tables_month_partitioned:
            query = query_tamplate.format(table)
            with connections[get_icds_ucr_citus_db_alias()].cursor() as db_cursor:
                db_cursor.execute(query)
                child_tables = [row[0] for row in db_cursor.fetchall()]
                all_agg_child_tables += child_tables

                child_tables = ['-t "{}"'.format(row[0]) for row in db_cursor.fetchall()]
                dump_query = dump_query_template.format(
                    child_tables=' '.join(child_tables),
                    parent_table=table
                )

                with open('pg_dump_script.sh', 'a') as fout:
                    fout.write(f"\n\n\n\n## TABLE NAME: {table}")
                    fout.write(dump_query)

        return all_agg_child_tables

    def get_agg_tables_state_partitioned(self):

        months_required = [
            '2020-09-01',
            '2020-08-01',
            '2020-07-01',
            '2020-06-01',
        ]

        # Shell command to dump the schema of tables
        dump_query_template = """
        sudo -u postgres pg_dump -d icds_ucr -s {child_tables} > {parent_table}.sql
        """
        all_agg_child_tables = []
        for tablename, child_table_prefix in self.agg_tables_month_partitioned:
            hashes = [hashlib.md5((self.state_id + month).encode('utf-8')).hexdigest()[8:] for month in months_required]
            child_tables = [ child_table_prefix + hash for hash in hashes]
            all_agg_child_tables += child_tables

            dump_query = dump_query_template.format(
                child_tables=' '.join(child_tables),
                parent_table=tablename
            )

            with open('pg_dump_script_intermediary.sh', 'a') as fout:
                fout.write(f"\n\n\n\n## TABLE NAME: {table}")
                fout.write(dump_query)

        return all_agg_child_tables
