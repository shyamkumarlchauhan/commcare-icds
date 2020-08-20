from mock import patch

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


@patch('custom.icds_reports.ucr.expressions._get_user_location_id',
       lambda user_id: 'qwe56poiuytr4xcvbnmkjfghwerffdaa')
@patch('corehq.apps.locations.ucr_expressions._get_location_type_name',
       lambda loc_id, context: 'awc')
class TestTHRSNForm(BaseFormsTest):
    ucr_name = "static-icds-cas-static_dashboard_thr_sn_forms"

    def test_thr_sn_form(self):
        self._test_data_source_results(
            'thr_sn_form',
            [{'doc_id': None, 'timeend': None,
              'ag_care_case_id': '7408dd27-884f-46b0-b32b-05fd7e960dc1',
              'check_ag_given_meal': 25}]
            )
