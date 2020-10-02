
from custom.icds_reports.const import AGG_PERSON_CASE_TABLE, AGG_MIGRATION_TABLE, AGG_AVAILING_SERVICES_TABLE
from custom.icds_reports.utils.aggregation_helpers.distributed.base import BaseICDSAggregationDistributedHelper
from corehq.apps.userreports.util import get_table_name
from dateutil.relativedelta import relativedelta
from custom.icds_reports.utils.aggregation_helpers import transform_day_to_month, month_formatter
from corehq.apps.locations.models import SQLLocation


class PersonCaseAggregationDistributedHelper(BaseICDSAggregationDistributedHelper):
    helper_key = 'agg-person-cases'
    tablename = AGG_PERSON_CASE_TABLE

    def __init__(self, month):
        self.month_start = transform_day_to_month(month)
        self.month_end = self.month_start + relativedelta(months=1, seconds=-1)
        self.next_month_start = month + relativedelta(months=1)
        self.person_case_ucr = get_table_name(self.domain, 'static-person_cases_v3')

        self.current_month_table = self.monthly_tablename()

    def aggregate(self, cursor):
        drop_older_table = self.drop_old_tables_query()
        create_query = self.create_table_query()
        agg_query = self.aggregation_query()
        add_partition_query = self.add_partition_table__query()

        cursor.execute(drop_older_table)
        cursor.execute(create_query)
        cursor.execute(agg_query)

        cursor.execute(add_partition_query)

    def drop_old_tables_query(self):
        previous_month = self.month_start - relativedelta(months=1)

        return f"""
            DROP TABLE IF EXISTS "{self.monthly_tablename(previous_month)}";
            DROP TABLE IF EXISTS "{self.monthly_tablename(self.month_start)}"

        """

    def create_table_query(self):
        return f"""
            CREATE TABLE "{self.current_month_table}" (LIKE {self.tablename});
            SELECT create_distributed_table('{self.current_month_table}', 'supervisor_id');
        """

    def monthly_tablename(self, month=None):
        if not month:
            month = self.month_start

        return f"{self.tablename}_{month_formatter(month)}"

    @property
    def referral_columns(self):
        referred_in_month = f"(last_referral_date>='{self.month_start}' AND last_referral_date<'{self.next_month_start}')"
        referral_problems_col_names = [
            (('premature',), 'premature'),
            (('sepsis',), 'sepsis'),
            (('diarrhoea',), 'diarrhoea'),
            (('pneumonia',), 'pneumonia'),
            (('fever_child',), 'fever'),
            (('severely_underweight',), 'severely_underweight'),
            (('other_child',), 'other_child'),
            (('bleeding',), 'bleeding'),
            (('convulsions',), 'convulsions'),
            (('prolonged_labor',), 'prolonged_labor'),
            (('abortion_complications',), 'abortion_complications'),
            (('fever', 'offensive_discharge'), 'fever_discharge'),
            (('swelling', 'blurred_vision', 'other'), 'other'),
        ]

        columns = []
        for ref_problems, col_name in referral_problems_col_names:
            referral_col_name = f"total_{col_name}_referrals"
            referral_calculation = ["referral_health_problem like '%{prob}%'" for prob in ref_problems]
            referral_calculation = " OR ".join(referral_calculation)
            referral_calculation = f"({referral_calculation})"
            reached_facility_col_name = f"total_{col_name}_reached_facility"
            reached_facility_calculation = f"({referral_calculation} AND referral_reached_facility=1)"
            columns += [
                (referral_col_name, f"SUM(CASE WHEN {referral_calculation} AND {referred_in_month} THEN 1 ELSE 0 END)"),
                (reached_facility_col_name,
                 f"SUM(CASE WHEN {reached_facility_calculation} AND {referred_in_month} THEN 1 ELSE 0 END)"),
            ]
        return columns


    def aggregation_query(self):
        month_start_string = month_formatter(self.month_start)
        month_end_11yr = self.month_end - relativedelta(years=11)
        month_start_15yr = self.month_start - relativedelta(years=15)
        month_start_14yr = self.month_start - relativedelta(years=14)
        month_end_15yr = self.month_end - relativedelta(years=15)
        month_start_18yr = self.month_start - relativedelta(years=18)
        unmigrated = ("(agg_migration.is_migrated IS DISTINCT FROM 1 OR "
            		  f"agg_migration.migration_date::date >= '{self.month_start}')")
        seeking_services = (
        	"((agg_availing.is_registered IS DISTINCT FROM 0 OR "
            f"agg_availing.registration_date::date >= '{self.month_start}') AND {unmigrated})"
            )
        girls_11_14 = (
        	f"'{month_end_11yr}' > dob AND "
        	f"'{month_start_14yr}'<= dob AND sex = 'F'"
        	)
        girls_15_18 = (
        	f"'{month_end_15yr}' > dob AND "
        	f"'{month_start_18yr}'<= dob AND sex = 'F'"
        	)


        columns = [
        	('state_id', 'ucr.state_id'),
        	('supervisor_id', 'ucr.supervisor_id'),
        	('awc_id', 'ucr.awc_id'),
        	('month', f"'{self.month_start}'"),
        	('cases_person', f"SUM(CASE WHEN {seeking_services} THEN 1 else 0 END)"),
        	('cases_person_all', 'count(*)'),
        	('cases_person_adolescent_girls_11_14', f"SUM(CASE WHEN {girls_11_14} and {seeking_services} THEN 1 ELSE 0 END)"),
        	('cases_person_adolescent_girls_11_14_all', f"SUM(CASE WHEN {girls_11_14} THEN 1 ELSE 0 END)"),
        	('cases_person_adolescent_girls_11_14_all_v2', f"SUM(CASE WHEN {girls_11_14} AND {unmigrated} THEN 1 ELSE 0 END)"),
        	('cases_person_adolescent_girls_15_18', f"SUM(CASE WHEN {girls_15_18} AND {seeking_services} THEN 1 ELSE 0 END)"),
        	('cases_person_adolescent_girls_15_18_all', f"SUM(CASE WHEN {girls_15_18} AND {unmigrated}THEN 1 ELSE 0 END)"),
        	('cases_person_referred', f"SUM(CASE WHEN last_referral_date>='{self.month_start}' AND last_referral_date<'{self.next_month_start}' THEN 1 ELSE 0 END)"),
        	]

        columns += self.referral_columns

        column_names = ", ".join([col[0] for col in columns])
        calculations = ", ".join([col[1] for col in columns])

        return f"""
                INSERT INTO "{self.current_month_table}" (
                    {column_names}
                )
                (
                	SELECT
                		{calculations}
	                FROM 
	                "{self.person_case_ucr}" ucr LEFT JOIN 
	                "{AGG_MIGRATION_TABLE}" agg_migration ON (
                		ucr.doc_id = agg_migration.person_case_id AND
                		agg_migration.month = '{self.month_start}' AND
                		ucr.supervisor_id = agg_migration.supervisor_id) LEFT JOIN 
                	"{AGG_AVAILING_SERVICES_TABLE}" agg_availing ON (
                		ucr.doc_id = agg_availing.person_case_id AND
                		agg_availing.month = '{self.month_start}' AND
                		ucr.supervisor_id = agg_availing.supervisor_id)
        			WHERE (
        				opened_on <'{self.next_month_start}' AND
              			(closed_on IS NULL OR closed_on >= '{self.month_start}')
              			)
        			GROUP BY ucr.state_id,ucr.supervisor_id, ucr.awc_id
              	)
                """


    def add_partition_table__query(self):
        return f"""
            ALTER TABLE "{self.tablename}" ATTACH PARTITION "{self.current_month_table}"
            FOR VALUES IN ('{month_formatter(self.month_start)}')
        """
