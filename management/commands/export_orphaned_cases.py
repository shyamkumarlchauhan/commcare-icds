import csv

from django.core.management import BaseCommand

from corehq.apps.es import CaseES, filters
from corehq.apps.locations.models import SQLLocation

from dimagi.utils.chunked import chunked

from corehq.util.log import with_progress_bar

CHILD_PROPERTIES = ['case_id', 'owner_id', 'opened_on', 'server_modified_on',
                    'name', 'aadhar_number', 'dob', 'died']

SOURCE_FIELDS = CHILD_PROPERTIES + ['indices']

CSV_HEADERS = CHILD_PROPERTIES + ['owner_name']


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            'hh_file',
            help="File path for household case output file",
        )
        parser.add_argument(
            'child_file',
            help="File path child case output file",
        )

    def handle(self, hh_file, child_file, **options):
        relevant_districts = SQLLocation.objects.filter(domain='icds-cas',
                                                        location_id__in=['fd0e25031bab6d778d7391c1d1ba715a',
                                                                         'a2fcb186e9be8464e167bb1c56ce8260',
                                                                         '46befa842d7484d50097c810f4d592cf',
                                                                         '4049fded0acc987d53cfc23173cf2bb0',
                                                                         'd982a6fb4cca0824fbde59db18d3721b'])
        owners = SQLLocation.objects.get_queryset_descendants(relevant_districts, include_self=True)
	    owner_name_mapping = {loc.location_id: loc.name for loc in owners}
	    hh_cases = self._get_closed_hh_cases(owner_name_mapping.keys())
        with open(hh_file, 'w') as hh_csv, open(child_file, 'w') as child_csv:
            hh_writer = csv.writer(hh_csv)
            child_writer = csv.writer(child_csv)
            hh_writer.writerow(['case_id', 'closed_on'])
            child_writer.writerow(CSV_HEADERS)
            for cases in chunked(with_progress_bar(hh_cases, hh_cases.count), 500):
                household_ids = []
                for hh in cases:
                    hh_writer.writerow([hh['case_id'], hh.get('closed_on', '')])
                    household_ids.append(hh['case_id'])
                child_cases = self._get_child_cases(household_ids)
                ids = set(household_ids)
                for child in child_cases.hits:
                    if any(True for index in child['indices'] if index['referenced_id'] in ids and index['identifier'] == 'parent'):
                        row = [child.get(prop, '').encode('utf-8') for prop in CHILD_PROPERTIES]
                        row.append(owner_name_mapping.get(child.get('owner_id', ''), '').encode('utf-8'))
			child_writer.writerow(row)

    def _get_closed_hh_cases(self, owners):
        query = (CaseES(es_instance_alias='export')
                 .is_closed()
                 .domain('icds-cas')
                 .case_type('household')
                 .owner(owners)
		         .source(['case_id', 'closed_on'])
                 .size(100)
                 )
        return query.scroll()

    def _get_child_cases(self, household_ids):
        query = (CaseES(es_instance_alias='export')
                 .domain('icds-cas')
                 .case_type('person')
                 .source(SOURCE_FIELDS)
                 .filter(filters.term("indices.referenced_id", household_ids))
                )
        return query.run()
