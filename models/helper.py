from __future__ import absolute_import, unicode_literals

from django.db import models

from corehq.blobs import CODES, get_blob_db
from corehq.blobs.models import BlobMeta
from custom.icds_reports.const import DASHBOARD_DOMAIN


EXPIRED = 60 * 60 * 24 * 7  # 7 days


class IcdsMonths(models.Model):
    month_name = models.TextField(primary_key=True)
    start_date = models.DateField()
    end_date = models.DateField()

    class Meta:
        managed = False
        db_table = 'icds_months'


class IcdsFile(models.Model):
    blob_id = models.CharField(max_length=255)
    data_type = models.CharField(max_length=255)
    file_added = models.DateField(auto_now=True)

    def store_file_in_blobdb(self, file, expired=EXPIRED, name=None):
        db = get_blob_db()
        try:
            kw = {"meta": db.metadb.get(
                parent_id='IcdsFile',
                key=self.blob_id
            )}
        except BlobMeta.DoesNotExist:
            kw = {
                "domain": DASHBOARD_DOMAIN,
                "parent_id": 'IcdsFile',
                "type_code": CODES.tempfile,
                "key": self.blob_id,
                "timeout": expired,
            }
            if name:
                kw["name"] = name
        db.put(file, **kw)

    def get_file_from_blobdb(self):
        return get_blob_db().get(key=self.blob_id)

    def get_file_size(self):
        return get_blob_db().size(key=self.blob_id)

    class Meta:
        app_label = 'icds_reports'
