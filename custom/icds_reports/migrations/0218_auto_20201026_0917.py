# Generated by Django 2.2.13 on 2020-10-26 09:17

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0217_rebuild_db_views'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='aggservicedeliveryreport',
            name='awc_days_open',
        ),
        migrations.RemoveField(
            model_name='aggservicedeliveryreport',
            name='awc_num_open',
        ),
    ]
