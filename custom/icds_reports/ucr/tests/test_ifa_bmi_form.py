from mock import patch

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


@patch('custom.icds_reports.ucr.expressions._get_user_location_id',
       lambda user_id: 'qwe56poiuytr4xcvbnmkjfghwerffdaa')
@patch('corehq.apps.locations.ucr_expressions._get_location_type_name',
       lambda loc_id, context: 'awc')
class TestTHRSNForm(BaseFormsTest):
    ucr_name = "static-icds-cas-static_dashboard_ifa_bmi_forms"

    def test_thr_sn_form(self):
        self._test_data_source_results(
            'ifa_bmi_form',
            [{'doc_id': None, 'timeend': None, 'ag_care_case_id': 'ba4e50ed-f911-48ca-a8ea-2e4662d77b26',
              'bmi_grading': 'white'}]
        )
