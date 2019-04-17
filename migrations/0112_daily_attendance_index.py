# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations


def get_operations():
    if not settings.UNIT_TESTING:
        return []

    # only do this in unit tests - we'll manage the rollout manually
    return [
        migrations.RunSQL("CREATE INDEX idx_daily_attendance_awc_id ON daily_attendance (awc_id)")
    ]


class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0111_service_delivery_dashboard'),
    ]

    operations = get_operations()
