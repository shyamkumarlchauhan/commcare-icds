# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-04-21 20:17
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0183_auto_20200420_1038'),
    ]

    operations = [
        migrations.AlterField(
            model_name='aggregateavailingserviceforms',
            name='state_id',
            field=models.TextField(null=True),
        ),
    ]
