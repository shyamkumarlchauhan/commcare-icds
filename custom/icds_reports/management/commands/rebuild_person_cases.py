from django.core.management.base import BaseCommand
from corehq.apps.userreports.models import AsyncIndicator
from django.db import connections

from dimagi.utils.chunked import chunked


class Command(BaseCommand):
    help = "Rebuild Person Cases with missing dobs"

    def handle(self, *args, **kwargs):
        query = """
            SELECT DISTINCT chm.child_person_case_id FROM child_health_monthly chm
            LEFT JOIN "ucr_icds-cas_static-person_cases_v3_2ae0879a" person_case ON (
                chm.child_person_case_id = person_case.doc_id AND
                chm.state_id = person_case.state_id AND
                chm.supervisor_id = person_case.supervisor_id
            ) WHERE (chm.month='2020-07-01' or chm.month='2020-08-01') AND person_case.doc_id IS NULL
            AND chm.child_person_case_id IS NOT NULL AND chm.child_person_case_id != '';
        """

        with connections['icds-ucr-citus'].cursor() as cursor:
            cursor.execute(query)
            doc_ids = cursor.fetchall()
            ids = [elem[0] for elem in doc_ids]
        ids.sort()
        total_doc_ids = len(ids)
        count = 0
        chunk_size = 1000
        for ids_chunk in chunked(ids, chunk_size):
            ids_list = list(ids_chunk)
            AsyncIndicator.bulk_creation([elem for elem in ids_list], 'CommCareCase', 'icds-cas', ['static-icds-cas-static-person_cases_v3'])
            count += chunk_size
            print("Success till doc_id: {}".format(ids_list[-1]))
            print("progress: {}/{}".format(count, total_doc_ids))
