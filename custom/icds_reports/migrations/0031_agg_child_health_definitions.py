# Generated by Django 1.11.9 on 2018-01-15 19:47
from corehq.sql_db.operations import RawSQLMigration
from django.db import migrations

from custom.icds_reports.const import SQL_TEMPLATES_ROOT

migrator = RawSQLMigration((SQL_TEMPLATES_ROOT,))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0030_auto_20171128_0042'),
    ]

    operations = [
        migrator.get_migration('update_tables15.sql'),
    ]
