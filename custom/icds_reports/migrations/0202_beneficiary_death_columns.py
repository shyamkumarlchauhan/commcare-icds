# -*- coding: utf-8 -*-
# Generated by Django 1.11.28
from __future__ import unicode_literals


from __future__ import unicode_literals

from django.db import migrations, models
from custom.icds_reports.utils.migrations import get_view_migrations
from custom.icds_reports.utils.migrations import (
    get_composite_primary_key_migrations,
)
import custom.icds_reports.models.aggregate


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0201_auto_20200817_1631'),
    ]

    operations = [
        migrations.RunSQL("ALTER TABLE ccs_record_monthly ADD COLUMN female_death_type text"),
        migrations.RunSQL("ALTER TABLE agg_ccs_record ADD COLUMN death_during_preg integer"),
        migrations.RunSQL("ALTER TABLE agg_ccs_record ADD COLUMN death_during_delivery integer"),
        migrations.RunSQL("ALTER TABLE agg_ccs_record ADD COLUMN death_pnc integer"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN live_birth smallint"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN still_birth smallint"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN weighed_within_3_days smallint"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN weighed_within_3_days integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN live_birth integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN still_birth integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN deaths integer"),
        migrations.AddField(
            model_name='aggregateawcinfrastructureforms',
            name='use_salt',
            field=models.PositiveSmallIntegerField(null=True),
        ),
        migrations.RunSQL('ALTER TABLE agg_awc ADD COLUMN use_salt integer'),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='awc_days_open',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='awc_num_open',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='breakfast_21_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='breakfast_9_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='breakfast_served',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='hcm_21_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='hcm_9_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='hcm_served',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='pse_16_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='pse_9_days',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='pse_provided',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggservicedeliveryreport',
            name='thr_served',
            field=models.IntegerField(null=True),
        ),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_bfast_count integer"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_hotcooked_count integer"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN days_thr_provided_count integer"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_pse_count integer"),

        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN total_pse_days_attended smallint"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_0_days integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_1_days integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN awc_open_with_attended_children integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN num_days_4_pse_activities integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN num_days_1_pse_activities integer"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_4_acts_count smallint"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_1_acts_count smallint"),
        migrations.CreateModel(
            name='AggMPRAwc',
            fields=[
                ('state_id', models.TextField(null=True)),
                ('district_id', models.TextField(null=True)),
                ('block_id', models.TextField(null=True)),
                ('supervisor_id', models.TextField(null=True)),
                ('awc_id', models.TextField(primary_key=True, serialize=False)),
                ('state_is_test', models.SmallIntegerField(null=True)),
                ('district_is_test', models.SmallIntegerField(null=True)),
                ('block_is_test', models.SmallIntegerField(null=True)),
                ('supervisor_is_test', models.SmallIntegerField(null=True)),
                ('awc_is_test', models.SmallIntegerField(null=True)),
                ('month', models.DateField(null=True)),
                ('aggregation_level', models.SmallIntegerField(null=True)),
                ('visitor_icds_sup', models.IntegerField(null=True)),
                ('visitor_anm', models.IntegerField(null=True)),
                ('visitor_health_sup', models.IntegerField(null=True)),
                ('visitor_cdpo', models.IntegerField(null=True)),
                ('visitor_med_officer', models.IntegerField(null=True)),
                ('visitor_dpo', models.IntegerField(null=True)),
                ('visitor_officer_state', models.IntegerField(null=True)),
                ('visitor_officer_central', models.IntegerField(null=True)),
                ('vhnd_done_when_planned', models.IntegerField(null=True)),
                ('vhnd_with_aww_present', models.IntegerField(null=True)),
                ('vhnd_with_icds_sup', models.IntegerField(null=True)),
                ('vhnd_with_asha_present', models.IntegerField(null=True)),
                ('vhnd_with_anm_mpw', models.IntegerField(null=True)),
                ('vhnd_with_health_edu_org', models.IntegerField(null=True)),
                ('vhnd_with_display_tools', models.IntegerField(null=True)),
                ('vhnd_with_thr_distr', models.IntegerField(null=True)),
                ('vhnd_with_child_immu', models.IntegerField(null=True)),
                ('vhnd_with_vit_a_given', models.IntegerField(null=True)),
                ('vhnd_with_anc_today', models.IntegerField(null=True)),
                ('vhnd_with_local_leader', models.IntegerField(null=True)),
                ('vhnd_with_due_list_prep_immunization', models.IntegerField(null=True)),
                ('vhnd_with_due_list_prep_vita_a', models.IntegerField(null=True)),
                ('vhnd_with_due_list_prep_antenatal_checkup', models.IntegerField(null=True)),
            ],
            options={
                'db_table': 'agg_mpr_awc'
            },
            bases=(models.Model, custom.icds_reports.models.aggregate.AggregateMixin),
        ),
        migrations.AlterUniqueTogether(
            name='aggmprawc',
            unique_together=set(
                [('month', 'aggregation_level', 'state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id')]),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_abortion_complications_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_bleeding_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_convulsions_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_diarrhoea_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_fever_discharge_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_fever_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_other_child_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_other_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_pneumonia_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_premature_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_prolonged_labor_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_sepsis_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='num_severely_underweight_referral_awcs',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_abortion_complications_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_abortion_complications_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_bleeding_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_bleeding_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_convulsions_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_convulsions_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_diarrhoea_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_diarrhoea_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_fever_discharge_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_fever_discharge_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_fever_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_fever_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_other_child_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_other_child_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_other_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_other_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_pneumonia_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_pneumonia_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_premature_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_premature_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_prolonged_labor_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_prolonged_labor_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_sepsis_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_sepsis_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_severely_underweight_reached_facility',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='total_severely_underweight_referrals',
            field=models.IntegerField(null=True),
        ),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_16_days_sc smallint"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_16_days_st integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_16_days_other integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_16_days_disabled integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_16_days_minority integer"),
        migrations.RunSQL('ALTER TABLE child_health_monthly ADD COLUMN fully_immun_before_month_end smallint'),
        migrations.RunSQL('ALTER TABLE agg_child_health ADD COLUMN fully_immun_before_month_end integer'),
        migrations.RunSQL('ALTER TABLE agg_child_health ADD COLUMN fully_immunized_eligible_in_month integer'),
        migrations.RunSQL("ALTER TABLE agg_ccs_record ADD COLUMN pregnant_permanent_resident smallint"),
        migrations.RunSQL("ALTER TABLE agg_ccs_record ADD COLUMN pregnant_temp_resident smallint"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN live_birth smallint"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN live_birth_permanent_resident integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN live_birth_temp_resident integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN permanent_resident integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN temp_resident integer"),
    ]

    operations.extend(get_view_migrations())
    operations.extend(get_composite_primary_key_migrations(['aggmprawc']))
