# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2018-10-31 13:50
from __future__ import unicode_literals
from __future__ import absolute_import

from django.db import migrations
from corehq.sql_db.operations import RawSQLMigration


migrator = RawSQLMigration(('custom', 'icds_reports', 'migrations', 'sql_templates'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0077_drop_agg_funcs'),
    ]

    operations = [
        migrator.get_migration('update_tables32.sql')
    ]
