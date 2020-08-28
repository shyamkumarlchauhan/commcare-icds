from dateutil.relativedelta import relativedelta

from corehq.apps.userreports.util import get_table_name
from custom.icds_reports.const import AGG_MPR_AWC_TABLE
from custom.icds_reports.utils.aggregation_helpers import  month_formatter, get_child_health_temp_tablename
from custom.icds_reports.utils.aggregation_helpers.distributed.base import AggregationPartitionedHelper


class AggMprAwcHelper(AggregationPartitionedHelper):
    helper_key = 'agg-mpr-awc'
    base_tablename = AGG_MPR_AWC_TABLE
    staging_tablename = f'staging_{AGG_MPR_AWC_TABLE}'

    @property
    def monthly_tablename(self):
        month_start = self.month.strftime("%Y-%m-%d")
        return f"{self.base_tablename}_{month_start}"

    @property
    def previous_agg_table_name(self):
        return f"previous_{self.monthly_tablename}"

    @property
    def temporary_tablename(self):
        return f"temp_{self.monthly_tablename}"

    @property
    def model(self):
        from custom.icds_reports.models.aggregate import AggMPRAwc
        return AggMPRAwc

    def create_temporary_table(self):
        return f"""
        CREATE UNLOGGED TABLE \"{self.temporary_tablename}\" (LIKE {self.base_tablename} INCLUDING INDEXES);
        SELECT create_distributed_table('{self.temporary_tablename}', 'supervisor_id');
        """

    @property
    def visitorbook_ucr_table(self):
        return get_table_name(self.domain, 'static-visitorbook_forms')

    @property
    def awc_vhnd_ucr_table(self):
        return get_table_name(self.domain, 'static-vhnd_form')

    @property
    def person_case_ucr_table(self):
        return get_table_name(self.domain, 'static-person_cases_v3')

    def drop_temporary_table(self):
        return f"""DROP TABLE IF EXISTS \"{self.temporary_tablename}\";"""

    def staging_queries(self):
        month_start_string = month_formatter(self.month)
        next_month_start = month_formatter(self.month + relativedelta(months=1))
        columns = (
            ('state_id', 'awc_location.state_id'),
            ('district_id', 'awc_location.district_id'),
            ('block_id', 'awc_location.block_id'),
            ('supervisor_id', 'awc_location.supervisor_id'),
            ('awc_id', 'awc_location.doc_id'),
            ('month', f"'{month_start_string}'"),
            ('aggregation_level', '5'),
            ('state_is_test','awc_location.state_is_test'),
            ('district_is_test','awc_location.district_is_test'),
            ('block_is_test','awc_location.block_is_test'),
            ('supervisor_is_test','awc_location.supervisor_is_test'),
            ('awc_is_test','awc_location.awc_is_test')
        )

        column_names = ", ".join([col[0] for col in columns])
        calculations = ", ".join([col[1] for col in columns])

        yield f"""
                INSERT INTO "{self.temporary_tablename}" (
                    {column_names}
                )
                (
                SELECT
                {calculations}
                from awc_location
                WHERE awc_location.aggregation_level=5);
                """,{
        }

        yield f"""
        UPDATE "{self.temporary_tablename}" agg_mpr
        SET
            visitor_icds_sup = ut.visitor_icds_sup,
            visitor_anm = ut.visitor_anm,
            visitor_health_sup = ut.visitor_health_sup,
            visitor_cdpo = ut.visitor_cdpo,
            visitor_med_officer = ut.visitor_med_officer,
            visitor_dpo = ut.visitor_dpo,
            visitor_officer_state = ut.visitor_officer_state,
            visitor_officer_central = ut.visitor_officer_central
        FROM (
           SELECT
               awc_id,
               CASE WHEN SUM(visitor_icds_sup)>=1 THEN 1 ELSE 0 END AS visitor_icds_sup,
               CASE WHEN SUM(visitor_anm)>=1 THEN 1 ELSE 0 END AS visitor_anm,
               CASE WHEN SUM(visitor_health_sup)>=1 THEN 1 ELSE 0 END AS visitor_health_sup,
               CASE WHEN SUM(visitor_cdpo)>=1 THEN 1 ELSE 0 END AS visitor_cdpo,
               CASE WHEN SUM(visitor_med_officer)>=1 THEN 1 ELSE 0 END AS visitor_med_officer,
               CASE WHEN SUM(visitor_dpo)>=1 THEN 1 ELSE 0 END AS visitor_dpo,
               CASE WHEN SUM(visitor_officer_state)>=1 THEN 1 ELSE 0 END AS visitor_officer_state,
               CASE WHEN SUM(visitor_officer_central)>=1 THEN 1 ELSE 0 END AS visitor_officer_central
           FROM "{self.visitorbook_ucr_table}" ucr
           WHERE submitted_on>=%(start_date)s and submitted_on<%(next_month_start_date)s
           GROUP BY awc_id
        ) ut
        
        WHERE agg_mpr.month=%(start_date)s AND
            agg_mpr.awc_id=ut.awc_id AND
            agg_mpr.aggregation_level=5
        """, {
            'start_date': self.month,
            'next_month_start_date': next_month_start
        }

        yield f"""
        UPDATE  "{self.temporary_tablename}" agg_mpr
        SET
            vhnd_done_when_planned = ut.done_when_planned,
            vhnd_with_aww_present = ut.aww_present,
            vhnd_with_icds_sup = ut.icds_sup,
            vhnd_with_asha_present = ut.asha_present,
            vhnd_with_anm_mpw = ut.anm_mpw,
            vhnd_with_health_edu_org = ut.health_edu_org,
            vhnd_with_display_tools = ut.display_tools,
            vhnd_with_thr_distr = ut.thr_distr,
            vhnd_with_child_immu = ut.child_immu,
            vhnd_with_vit_a_given = ut.vit_a_given,
            vhnd_with_anc_today = ut.anc_today,
            vhnd_with_local_leader = ut.local_leader,
            vhnd_with_due_list_prep_immunization = ut.due_list_prep_immunization,
            vhnd_with_due_list_prep_vita_a = ut.due_list_prep_vit_a,
            vhnd_with_due_list_prep_antenatal_checkup = ut.due_list_prep_antenatal_checkup
        FROM (
           SELECT
               awc_id,
               CASE WHEN SUM(done_when_planned)>=1 THEN 1 ELSE 0 END AS done_when_planned,
               CASE WHEN SUM(aww_present)>=1 THEN 1 ELSE 0 END AS aww_present,
               CASE WHEN SUM(icds_sup)>=1 THEN 1 ELSE 0 END AS icds_sup,
               CASE WHEN SUM(asha_present)>=1 THEN 1 ELSE 0 END AS asha_present,
               CASE WHEN SUM(anm_mpw)>=1 THEN 1 ELSE 0 END AS anm_mpw,
               CASE WHEN SUM(health_edu_org)>=1 THEN 1 ELSE 0 END AS health_edu_org,
               CASE WHEN SUM(display_tools)>=1 THEN 1 ELSE 0 END AS display_tools,
               CASE WHEN SUM(thr_distr)>=1 THEN 1 ELSE 0 END AS thr_distr,
               CASE WHEN SUM(child_immu)>=1 THEN 1 ELSE 0 END AS child_immu,
               CASE WHEN SUM(vit_a_given)>=1 THEN 1 ELSE 0 END AS vit_a_given,
               CASE WHEN SUM(anc_today)>=1 THEN 1 ELSE 0 END AS anc_today,
               CASE WHEN SUM(local_leader)>=1 THEN 1 ELSE 0 END AS local_leader,
               CASE WHEN SUM(due_list_prep_immunization)>=1 THEN 1 ELSE 0 END AS due_list_prep_immunization,
               CASE WHEN SUM(due_list_prep_vit_a)>=1 THEN 1 ELSE 0 END AS due_list_prep_vit_a,
               CASE WHEN SUM(due_list_prep_antenatal_checkup)>=1 THEN 1 ELSE 0 END AS due_list_prep_antenatal_checkup
           FROM "{self.awc_vhnd_ucr_table}" ucr
           WHERE submitted_on>=%(start_date)s and submitted_on<%(next_month_start_date)s
           GROUP BY awc_id
        ) ut

        WHERE agg_mpr.month=%(start_date)s AND
            agg_mpr.awc_id=ut.awc_id AND
            agg_mpr.aggregation_level=5
        """, {
            'start_date': self.month,
            'next_month_start_date': next_month_start
        }



        yield f"""
        UPDATE  "{self.temporary_tablename}" agg_mpr
            SET 
            mother_death_permanent_resident = ut.female_dead_resident,
            mother_death_temp_resident = ut.female_dead_migrant,
            pregnancy_death_permanent_resident = ut.pregnancy_dead_resident,
            pregnancy_death_temp_resident = ut.pregnancy_dead_migrant,
            delivery_death_permanent_resident = ut.delivery_dead_resident,
            delivery_death_temp_resident = ut.delivery_dead_migrant,
            pnc_death_permanent_resident = ut.pnc_dead_resident,
            pnc_death_temp_resident = ut.pnc_dead_migrant
        FROM (
        SELECT
            supervisor_id,
            awc_id,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.age_at_death_yrs >= 11) AS female_dead_resident,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident is distinct from 1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.age_at_death_yrs >= 11) AS female_dead_migrant,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='pregnant') AS pregnancy_dead_resident,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident IS DISTINCT FROM 1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='pregnant') AS pregnancy_dead_migrant,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='delivery') AS delivery_dead_resident,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='delivery') AS delivery_dead_migrant,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='pnc') AS pnc_dead_resident,
            COUNT(*) filter (WHERE ucr.sex='F' AND ucr.resident=1 AND ucr.date_death IS NOT NULL 
                                AND ucr.date_death>=%(start_date)s AND ucr.date_death<%(next_month_start_date)s 
                                AND ucr.female_death_type='pnc') AS pnc_dead_migrant
        
        FROM "{self.person_case_ucr_table}" ucr LEFT JOIN "{migration_table}" agg_migration ON (
                ucr.doc_id = agg_migration.person_case_id AND
                agg_migration.month = %(start_date)s AND
                ucr.supervisor_id = agg_migration.supervisor_id
             ) LEFT JOIN "{availing_services_table}" agg_availing ON (
                ucr.doc_id = agg_availing.person_case_id AND
                agg_availing.month = %(start_date)s AND
                ucr.supervisor_id = agg_availing.supervisor_id
             )
        WHERE (opened_on <= %(end_date)s AND
              (closed_on IS NULL OR closed_on >= %(start_date)s)) AND 
              (agg_availing.is_registered IS DISTINCT FROM 0 OR agg_availing.registration_date::date >= %(start_date)s) AND 
              (agg_migration.is_migrated IS DISTINCT FROM 1 OR agg_migration.migration_date::date >= %(start_date)s)
        GROUP BY supervisor_id, awc_id
        ) ut
        WHERE 
            agg_mpr.supervisor_id=ut.supervisor_id AND
            agg_mpr.awc_id=ut.awc_id AND
            agg_mpr.aggregation_level=5 AND
            agg_mpr.month=%(start_date)s
        """
    def update_queries(self):

        yield f"""
            DROP TABLE IF EXISTS "local_tmp_agg_sdr";
            """, {
        }

        yield f"""
            CREATE TABLE "local_tmp_agg_sdr" AS SELECT * FROM "{self.temporary_tablename}";
            """, {
        }

        yield f"""
            INSERT INTO "{self.staging_tablename}" SELECT * from "local_tmp_agg_sdr";
            """, {
        }
        yield f"""
            DROP TABLE "local_tmp_agg_sdr";
            """, {
        }

    def rollup_query(self, aggregation_level):
        from custom.icds_reports.utils import (
            create_group_by, test_column_name,
            prepare_rollup_query, column_value_as_per_agg_level
        )
        columns = (
            ('state_id', 'state_id'),
            ('district_id', column_value_as_per_agg_level(aggregation_level, 1,'district_id', "'All'") ),
            ('block_id', column_value_as_per_agg_level(aggregation_level, 2,'block_id', "'All'")),
            ('supervisor_id', column_value_as_per_agg_level(aggregation_level, 3,'supervisor_id', "'All'")),
            ('awc_id', column_value_as_per_agg_level(aggregation_level, 4,'awc_id', "'All'")),
            ('aggregation_level', str(aggregation_level)),
            ('month', 'month'),
            ('visitor_icds_sup',),
            ('visitor_anm',),
            ('visitor_health_sup',),
            ('visitor_cdpo',),
            ('visitor_med_officer',),
            ('visitor_dpo',),
            ('visitor_officer_state',),
            ('visitor_officer_central',),
            ('vhnd_done_when_planned',),
            ('vhnd_with_aww_present',),
            ('vhnd_with_icds_sup',),
            ('vhnd_with_asha_present',),
            ('vhnd_with_anm_mpw',),
            ('vhnd_with_health_edu_org',),
            ('vhnd_with_display_tools',),
            ('vhnd_with_thr_distr',),
            ('vhnd_with_child_immu',),
            ('vhnd_with_vit_a_given',),
            ('vhnd_with_anc_today',),
            ('vhnd_with_local_leader',),
            ('vhnd_with_due_list_prep_immunization',),
            ('vhnd_with_due_list_prep_vita_a',),
            ('vhnd_with_due_list_prep_antenatal_checkup',),
            ('state_is_test', 'MAX(state_is_test)'),
            ('mother_death_permanent_resident',),
            ('mother_death_temp_resident',),
            ('pregnancy_death_permanent_resident',),
            ('pregnancy_death_temp_resident',),
            ('delivery_death_permanent_resident',),
            ('delivery_death_temp_resident',),
            ('pnc_death_permanent_resident',),
            ('pnc_death_temp_resident',),
            ('district_is_test', column_value_as_per_agg_level(aggregation_level, 1,'MAX(district_is_test)', "0")),
            ('block_is_test', column_value_as_per_agg_level(aggregation_level, 2,'MAX(block_is_test)', "0")),
            ('supervisor_is_test', column_value_as_per_agg_level(aggregation_level, 3,'MAX(supervisor_is_test)', "0")),
            ('awc_is_test', column_value_as_per_agg_level(aggregation_level, 3,'MAX(awc_is_test)', "0"))
        )

        column_names, calculations = prepare_rollup_query(columns)

        child_location_test_column_name = test_column_name(aggregation_level)

        group_by = create_group_by(aggregation_level)
        group_by.append('month')
        group_by_text = ", ".join(group_by)

        return f"""
        INSERT INTO "{self.staging_tablename}" (
            {column_names}
        ) (
            SELECT {calculations}
            FROM "{self.staging_tablename}"
            WHERE {child_location_test_column_name} = 0 AND aggregation_level = {aggregation_level + 1}
            GROUP BY {group_by_text}
            ORDER BY {group_by_text}
        )
        """

    def indexes(self):
        staging_tablename = self.staging_tablename
        return [
            f'CREATE INDEX ON "{staging_tablename}" (aggregation_level, state_id)',
            f'CREATE INDEX ON "{staging_tablename}" (aggregation_level, district_id) WHERE aggregation_level > 1',
            f'CREATE INDEX ON "{staging_tablename}" (aggregation_level, block_id) WHERE aggregation_level > 2',
            f'CREATE INDEX ON "{staging_tablename}" (aggregation_level, supervisor_id) WHERE aggregation_level > 3',
        ]
