# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-05-17 10:37
from __future__ import unicode_literals
from __future__ import absolute_import

from architect.commands import partition
import django.contrib.postgres.fields
import django.contrib.postgres.fields.jsonb
from django.db import migrations, models

from corehq.sql_db.operations import HqRunPython


def add_partitions(apps, schema_editor):
    partition.run({'module': 'custom.icds_reports.models'})


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0047_added_new_wasting_and_stunting_columns_to_views'),
    ]

    operations = [
        migrations.CreateModel(
            name='ICDSAuditEntryRecord',
            fields=[
                ('id', models.UUIDField(primary_key=True, serialize=False, unique=True)),
                ('username', models.EmailField(db_index=True, max_length=254)),
                ('assigned_location_ids', django.contrib.postgres.fields.ArrayField(
                    base_field=models.CharField(max_length=255), null=True, size=None)
                 ),
                ('ip_address', models.GenericIPAddressField(null=True)),
                ('url', models.TextField()),
                ('post_data', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
                ('get_data', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
                ('session_key', models.CharField(max_length=32)),
                ('time_of_use_start', models.DateTimeField(auto_now_add=True)),
                ('time_of_use_end', models.DateTimeField(null=True)),
            ],
            options={
                'db_table': 'icds_audit_entry_record',
            },
        ),
        HqRunPython(add_partitions),
    ]
