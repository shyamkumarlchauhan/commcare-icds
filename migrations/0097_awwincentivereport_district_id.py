# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-02-15 13:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0096_valid_visits'),
    ]

    operations = [
        migrations.AddField(
            model_name='awwincentivereport',
            name='district_id',
            field=models.TextField(blank=True, null=True),
        ),
    ]
