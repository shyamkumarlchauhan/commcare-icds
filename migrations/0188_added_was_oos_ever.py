# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-05-13 15:47
from __future__ import unicode_literals

from django.db import migrations, models

from custom.icds_reports.utils.migrations import get_view_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0187_chm_view_growth_tracker'),
    ]

    operations = [
        migrations.AddField(
            model_name='biharapidemographics',
            name='was_oos_ever',
            field=models.SmallIntegerField(null=True),
        ),
    ]

    operations.extend(get_view_migrations())
