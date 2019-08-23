# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-05-24 16:57

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds', '0003_auto_20190520_1859'),
    ]

    operations = [
        migrations.AlterField(
            model_name='hostedcczlink',
            name='identifier',
            field=models.CharField(
                db_index=True,
                max_length=255,
                unique=True,
                validators=[
                    django.core.validators.RegexValidator(
                        '^[a-z0-9_-]*$',
                        code='invalid_hosted_ccz_link_identifier',
                        message='must contain lowercase alphanumeric or - or _')]),
        ),
    ]
