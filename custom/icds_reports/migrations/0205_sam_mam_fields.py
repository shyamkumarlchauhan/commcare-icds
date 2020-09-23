# Generated by Django 2.2.13 on 2020-09-06 07:27

from django.db import migrations, models
from custom.icds_reports.utils.migrations import get_composite_primary_key_migrations
from custom.icds_reports.utils.migrations import get_view_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0204_daily_thr_photos'),
    ]

    operations = [
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN last_referral_date date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN referral_reached_date date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN last_referral_discharge_date date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN referral_health_problem text "),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN sam_mam_visit_date_1 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN sam_mam_visit_date_2 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN sam_mam_visit_date_3 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN sam_mam_visit_date_4 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN poshan_panchayat_date_1 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN poshan_panchayat_date_2 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN poshan_panchayat_date_3 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN poshan_panchayat_date_4 date"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN last_recorded_weight decimal"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN last_recorded_height decimal"),

        migrations.CreateModel(
            name='AggregateSamMamForm',
            fields=[
                ('state_id', models.TextField(null=True)),
                ('supervisor_id', models.TextField(null=True)),
                ('month', models.DateField(help_text='Will always be YYYY-MM-01')),
                ('child_health_case_id', models.TextField(primary_key=True, serialize=False)),
                ('sam_mam_visit_date_1', models.DateField(blank=True, null=True)),
                ('sam_mam_visit_date_2', models.DateField(blank=True, null=True)),
                ('sam_mam_visit_date_3', models.DateField(blank=True, null=True)),
                ('sam_mam_visit_date_4', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_1', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_2', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_3', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_4', models.DateField(blank=True, null=True)),
            ],
            options={
                'db_table': 'icds_dashboard_sam_mam_forms',
                'unique_together': {('month', 'state_id', 'supervisor_id', 'child_health_case_id')},
            },
        ),

    ]
    operations.extend(get_composite_primary_key_migrations(['aggregatesammamform']))
    operations.append(migrations.RunSQL(f"SELECT create_distributed_table('icds_dashboard_sam_mam_forms', 'supervisor_id')"))
