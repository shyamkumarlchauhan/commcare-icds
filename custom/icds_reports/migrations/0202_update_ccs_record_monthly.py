# Generated by Django 2.2.13 on 2020-09-01 06:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0201_auto_20200817_1631'),
    ]

    operations = [
        migrations.RunSQL("ALTER TABLE ccs_record_monthly ADD COLUMN bpl_apl text"),
        migrations.RunSQL("ALTER TABLE ccs_record_monthly ADD COLUMN religion text")
    ]