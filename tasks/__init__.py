from datetime import datetime, timedelta

from celery.schedules import crontab
from django.conf import settings

from corehq.blobs import CODES, get_blob_db
from corehq.blobs.models import BlobMeta
from corehq.sql_db.util import get_db_aliases_for_partitioned_query
from corehq.util.celery_utils import periodic_task_on_envs
from corehq.util.datadog.gauges import datadog_counter
from custom.icds.tasks.sms import send_monthly_sms_report  # noqa imported for celery
from custom.icds.tasks.hosted_ccz import setup_ccz_file_for_hosting  # noqa imported for celery


@periodic_task_on_envs(settings.ICDS_ENVS, run_every=crontab(minute=0, hour='22'))
def delete_old_images(cutoff=None):
    cutoff = cutoff or datetime.utcnow()
    max_age = cutoff - timedelta(days=90)
    db = get_blob_db()

    def _get_query(db_name, max_age=max_age):
        return BlobMeta.objects.using(db_name).filter(
            content_type='image/jpeg',
            type_code=CODES.form_attachment,
            domain='icds-cas',
            created_on__lt=max_age
        )

    run_again = False
    for db_name in get_db_aliases_for_partitioned_query():
        bytes_deleted = 0
        metas = list(_get_query(db_name)[:1000])
        if metas:
            for meta in metas:
                bytes_deleted += meta.content_length or 0
            db.bulk_delete(metas=metas)
            datadog_counter('commcare.icds_images.bytes_deleted', value=bytes_deleted)
            datadog_counter('commcare.icds_images.count_deleted', value=len(metas))
            run_again = True

    if run_again:
        delete_old_images.delay(cutoff)
