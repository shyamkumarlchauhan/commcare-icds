from datetime import datetime

from celery.schedules import crontab
from dateutil.relativedelta import relativedelta

import settings
from corehq.apps.userreports.models import InvalidUCRData
from corehq.util.celery_utils import periodic_task_on_envs


@periodic_task_on_envs(settings.ICDS_ENVS, run_every=crontab(hour=21))
def delete_old_sms_events():
    end_date = datetime.utcnow() - relativedelta(months=3)
    InvalidUCRData.objects.filter(date_created__lt=end_date).delete()
