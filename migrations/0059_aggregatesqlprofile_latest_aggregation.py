# -*- coding: utf-8 -*-
# Generated by Django 1.11.14 on 2018-08-27 06:08
from __future__ import unicode_literals
from __future__ import absolute_import
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0058_new_agg_ccs_columns'),
    ]

    operations = [
        migrations.AddField(
            model_name='aggregatesqlprofile',
            name='latest_aggregation',
            field=models.DateField(null=True),
        ),
    ]
