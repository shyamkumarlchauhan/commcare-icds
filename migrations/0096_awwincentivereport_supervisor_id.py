# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-02-20 07:08
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0095_institutional_delivery_ccs_record_monthly'),
    ]

    operations = [
        migrations.AddField(
            model_name='awwincentivereport',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
    ]
