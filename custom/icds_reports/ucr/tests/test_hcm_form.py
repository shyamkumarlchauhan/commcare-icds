from mock import patch

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


@patch('custom.icds_reports.ucr.expressions._get_user_location_id',
       lambda user_id: 'qwe56poiuytr4xcvbnmkjfghwerffdaa')
@patch('corehq.apps.locations.ucr_expressions._get_location_type_name',
       lambda loc_id, context: 'awc')
class TestHCMForm(BaseFormsTest):
    ucr_name = "static-icds-cas-static_dashboard_hcm_forms"

    def test_hcm_form(self):
        self._test_data_source_results(
            'hcm_form',
            [{'doc_id': None, 'repeat_iteration': 0, 'timeend': None,
              'ag_care_case_id': 'd2cf6698-c601-41e9-96b4-9ac7d7c8773e',
              'check_ag_given_meal': 1},
             {'doc_id': None, 'repeat_iteration': 1, 'timeend': None,
              'ag_care_case_id': 'f79514e9-3d85-45cb-8807-abbc04644553',
              'check_ag_given_meal': 1},
             {'doc_id': None, 'repeat_iteration': 2, 'timeend': None,
              'ag_care_case_id': '8a92cc7e-a9f2-418f-88fb-5802ae85e95e',
              'check_ag_given_meal': 1}]
        )
