# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-02-20 10:05
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0095_institutional_delivery_ccs_record_monthly'),
    ]

    operations = [
        migrations.AddField(
            model_name='aggregateawcinfrastructureforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='aggregatechildhealthdailyfeedingforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='aggregatechildhealthpostnatalcareforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='aggregatechildhealththrforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='aggregatecomplementaryfeedingforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='aggregategrowthmonitoringforms',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
        migrations.AddField(
            model_name='dailyattendance',
            name='supervisor_id',
            field=models.CharField(max_length=40, null=True),
        ),
    ]
