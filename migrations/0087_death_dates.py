# -*- coding: utf-8 -*-
# Generated by Django 1.11.16 on 2018-12-10 20:35

from django.db import migrations

from corehq.sql_db.operations import RawSQLMigration

migrator = RawSQLMigration(('custom', 'icds_reports', 'migrations', 'sql_templates'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0086_disha_aggregate'),
    ]

    operations = [
        migrator.get_migration('update_tables36.sql')
    ]
