from corehq.sql_db.operations import RawSQLMigration
from django.db import migrations

from custom.icds_reports.const import SQL_TEMPLATES_ROOT

migrator = RawSQLMigration((SQL_TEMPLATES_ROOT,))


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0004_fix_agg_awc'),
    ]

    operations = [
        migrator.get_migration('update_tables2.sql'),
    ]
