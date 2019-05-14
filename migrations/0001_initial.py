# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-05-14 14:12
from __future__ import unicode_literals

import django.core.validators
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CCZHosting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('app_id', models.CharField(max_length=255)),
                ('version', models.IntegerField()),
                ('profile_id', models.CharField(blank=True, max_length=255)),
                ('file_name', models.CharField(blank=True, max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name='CCZHostingLink',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('identifier', models.CharField(db_index=True, max_length=255, unique=True, validators=[django.core.validators.RegexValidator('^[a-z0-9]*$', code='invalid_lowercase_alphanumeric', message='must be lowercase alphanumeric')])),
                ('username', models.CharField(max_length=255)),
                ('password', models.CharField(max_length=255)),
                ('domain', models.CharField(max_length=255)),
                ('page_title', models.CharField(blank=True, max_length=255)),
            ],
        ),
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
        migrations.AlterUniqueTogether(
            name='cczhostingsupportingfile',
            unique_together=set([('domain', 'blob_id')]),
        ),
        migrations.AddField(
            model_name='cczhosting',
            name='link',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='icds.CCZHostingLink'),
        ),
        migrations.AlterUniqueTogether(
            name='cczhosting',
            unique_together=set([('link', 'app_id', 'version', 'profile_id')]),
        ),
    ]
