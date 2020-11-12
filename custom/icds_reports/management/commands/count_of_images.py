from django.core.management.base import BaseCommand
from corehq.blobs.models import BlobMeta
from corehq.sql_db.util import get_db_aliases_for_partitioned_query


class Command(BaseCommand):
    def handle(self, *args, **options):
        total_count = 0
        for dbname in get_db_aliases_for_partitioned_query():
            print(dbname)
            total_count += BlobMeta.objects.using(dbname).filter(content_type__contains='image').count()

        print(f"TOTAL COUNT {total_count}")
