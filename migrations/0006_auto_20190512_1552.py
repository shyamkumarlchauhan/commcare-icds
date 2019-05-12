# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-05-12 15:52
from __future__ import absolute_import
from __future__ import unicode_literals

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds', '0005_cczhostinglink_page_title'),
    ]

    operations = [
        migrations.CreateModel(
            name='CCZHostingSupportingFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('domain', models.CharField(db_index=True, max_length=255)),
                ('blob_id', models.CharField(db_index=True, max_length=255)),
                ('file_name', models.CharField(max_length=255)),
                ('file_type', models.IntegerField(choices=[(1, 'zip'), (2, 'document')])),
                ('display', models.IntegerField(choices=[(1, 'list'), (2, 'footer')])),
            ],
        ),
        migrations.AlterField(
            model_name='cczhostinglink',
            name='identifier',
            field=models.CharField(db_index=True, max_length=255, unique=True, validators=[django.core.validators.RegexValidator('^[a-z0-9]*$', code='invalid_lowercase_alphanumeric', message='must be lowercase alphanumeric')]),
        ),
        migrations.AlterUniqueTogether(
            name='cczhostingsupportingfile',
            unique_together=set([('domain', 'blob_id')]),
        ),
    ]
