# -*- coding: utf-8 -*-
# Generated by Django 1.11.16 on 2019-02-01 10:04
from __future__ import unicode_literals, absolute_import

from django.db import migrations
from corehq.sql_db.operations import RawSQLMigration

migrator = RawSQLMigration(('custom', 'icds_reports', 'migrations', 'sql_templates'))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0088_child_name'),
    ]

    operations = []
