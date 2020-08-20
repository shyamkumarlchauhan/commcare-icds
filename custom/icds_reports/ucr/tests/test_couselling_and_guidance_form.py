from mock import patch

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


@patch('custom.icds_reports.ucr.expressions._get_user_location_id',
       lambda user_id: 'qwe56poiuytr4xcvbnmkjfghwerffdaa')
@patch('corehq.apps.locations.ucr_expressions._get_location_type_name',
       lambda loc_id, context: 'awc')
class TestCounsellingGuidanceForm(BaseFormsTest):
    ucr_name = "static-icds-cas-static_dashboard_counselling_and_guidance_forms"

    def test_counselling_and_guidance_form(self):
        self._test_data_source_results(
            'counselling_and_guidance_form',
            [{'ag_care_case_id': 'c03432da-411a-45b9-918a-27082ea4fb6e',
              'counselling_session_conducted': 1,
              'doc_id': None,
              'repeat_iteration': 0,
              'timeend': None},
             {'ag_care_case_id': '139b95ea-008a-40ed-bb76-d5d6ecef12cd',
              'counselling_session_conducted': 1,
              'doc_id': None,
              'repeat_iteration': 1,
              'timeend': None}])
