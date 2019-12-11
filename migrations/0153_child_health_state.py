# -*- coding: utf-8 -*-
# Generated by Django 1.11.26 on 2019-12-04 05:51
from __future__ import unicode_literals

from django.db import migrations
from corehq.sql_db.operations import RawSQLMigration


migrator = RawSQLMigration(('custom', 'icds_reports', 'migrations', 'sql_templates', 'database_views'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0152_dashboarduseractivityreport'),
    ]

    operations = [
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN state_id text"),
        migrator.get_migration('child_health_monthly.sql')
    ]
