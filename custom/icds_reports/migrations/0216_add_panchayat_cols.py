# Generated by Django 2.2.13 on 2020-09-24 16:58

import custom.icds_reports.models.aggregate
from django.db import migrations, models
from custom.icds_reports.utils.migrations import get_composite_primary_key_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0215_birth_death_cols'),
    ]

    operations = [
        migrations.CreateModel(
            name='AggregateSamMamPanchayatForm',
            fields=[
                ('state_id', models.TextField(null=True)),
                ('supervisor_id', models.TextField(null=True)),
                ('month', models.DateField(help_text='Will always be YYYY-MM-01')),
                ('awc_id', models.TextField(primary_key=True, serialize=False)),
                ('poshan_panchayat_date_1', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_2', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_3', models.DateField(blank=True, null=True)),
                ('poshan_panchayat_date_4', models.DateField(blank=True, null=True)),
            ],
            options={
                'db_table': 'icds_dashboard_sam_mam_panchayat_forms',
                'unique_together': {('month', 'state_id', 'supervisor_id', 'awc_id')},
            },
            bases=(models.Model, custom.icds_reports.models.aggregate.AggregateMixin),
        ),
        migrations.RemoveField(
            model_name='aggregatesammamform',
            name='poshan_panchayat_date_1',
        ),
        migrations.RemoveField(
            model_name='aggregatesammamform',
            name='poshan_panchayat_date_2',
        ),
        migrations.RemoveField(
            model_name='aggregatesammamform',
            name='poshan_panchayat_date_3',
        ),
        migrations.RemoveField(
            model_name='aggregatesammamform',
            name='poshan_panchayat_date_4',
        )
    ]
    operations.extend(get_composite_primary_key_migrations(['aggregatesammampanchayatform']))
    operations.append(migrations.RunSQL(f"SELECT create_distributed_table('icds_dashboard_sam_mam_panchayat_forms', 'supervisor_id')"))