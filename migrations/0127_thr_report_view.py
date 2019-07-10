# -*- coding: utf-8 -*-
# Generated by Django 1.11.16 on 2019-06-24
from __future__ import unicode_literals
from __future__ import absolute_import

from django.db import migrations
from corehq.sql_db.operations import RawSQLMigration

migrator = RawSQLMigration(('custom', 'icds_reports', 'migrations', 'sql_templates', 'database_views'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0126_agg_awc_image_column'),
    ]

    operations = [
        migrator.get_migration('thr_report_view.sql')
    ]
