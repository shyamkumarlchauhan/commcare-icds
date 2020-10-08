# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-04-17 09:26
from __future__ import unicode_literals


from __future__ import unicode_literals

from django.db import migrations

from custom.icds_reports.utils.migrations import get_view_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0208_add_immu_col_to_chm'),
    ]

    operations = [
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN total_pse_days_attended integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_0_days integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_1_days integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN awc_open_with_attended_children integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN num_days_4_pse_activities integer"),
        migrations.RunSQL("ALTER TABLE agg_awc ADD COLUMN num_days_1_pse_activities integer"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_4_acts_count smallint"),
        migrations.RunSQL("ALTER TABLE daily_attendance ADD COLUMN open_1_acts_count smallint"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_25_days_sc integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_25_days_st integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_25_days_other integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_25_days_disabled integer"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN pse_attended_25_days_minority integer"),
    ]
