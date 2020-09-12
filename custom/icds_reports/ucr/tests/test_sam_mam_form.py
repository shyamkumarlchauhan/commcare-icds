from datetime import date
from mock import patch

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


@patch('custom.icds_reports.ucr.expressions._get_user_location_id',
       lambda user_id: 'qwe56poiuytr4xcvbnmkjfghwerffdaa')
@patch('corehq.apps.locations.ucr_expressions._get_location_type_name',
       lambda loc_id, context: 'awc')
class TestSamMamForm(BaseFormsTest):
    ucr_name = "static-icds-cas-static-sam_mam_visit"

    def test_sam_mam_form(self):
        self._test_data_source_results(
            'sam_mam_form',
            [{
                'child_health_case_id': '24cdc793-ec9c-44f0-b9f3-8751e0ff307e',
                'count': 1,
                "doc_id": None,
                "timeend": None,
                'last_visit_date': date(2020, 9, 5),
                'poshan_panchayat_date': date(2020, 8, 23),
              }
             ])
