# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-05-14 12:45
from __future__ import unicode_literals

from django.db import migrations

from corehq.sql_db.connections import is_citus_db
from custom.icds_reports.const import REFERENCE_TABLES, DISTRIBUTED_TABLES
from custom.icds_reports.utils.migrations import create_citus_distributed_table, create_citus_reference_table


def _distribute_citus_tables(apps, schema_editor):
    if not is_citus_db(schema_editor.connection):
        return

    for table, col in DISTRIBUTED_TABLES:
        create_citus_distributed_table(schema_editor.connection, table, col)

    for table in REFERENCE_TABLES:
        create_citus_reference_table(schema_editor.connection, table)


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0117_monthly_pk'),
    ]

    operations = [
        migrations.RunPython(_distribute_citus_tables, migrations.RunPython.noop)
    ]
