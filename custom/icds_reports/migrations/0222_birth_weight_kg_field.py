# Generated by Django 2.2.13 on 2020-08-17 16:31

from django.db import migrations, models

from custom.icds_reports.utils.migrations import get_view_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0221_aggregateinactiveawwview'),
    ]

    operations = [
    migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN birth_weight_kg decimal"),
    ]

    operations.extend(get_view_migrations())