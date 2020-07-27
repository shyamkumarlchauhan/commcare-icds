# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-04-16 19:55
from __future__ import unicode_literals

from corehq.sql_db.operations import RawSQLMigration
from django.db import migrations

from custom.icds_reports.const import SQL_TEMPLATES_ROOT

migrator = RawSQLMigration((SQL_TEMPLATES_ROOT, 'database_views'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0178_rebuild_chm_view'),
    ]

    operations = [
        migrations.RunSQL('DROP TABLE IF EXISTS bihar_api_demographics CASCADE')
    ]
