# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-10-15
from __future__ import unicode_literals

from django.db import migrations
from custom.icds_reports.utils.migrations import get_view_migrations


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0216_add_panchayat_cols'),
    ]

    operations = get_view_migrations()