from django.test import TestCase

from custom.icds_reports.reports.service_delivery_dashboard_data import get_service_delivery_report_data


class TestServiceDeliveryData(TestCase):

    def test_get_service_delivery_report_data_0_3(self):
        get_service_delivery_report_data.clear('icds-cas', 0, 10, None, False,
                                        {'aggregation_level': 1}, 2017, 5, 'pw_lw_children')
        data = get_service_delivery_report_data(
            'icds-cas',
            0,
            10,
            None,
            False,
            {
                'aggregation_level': 1,
            },
            2017,
            5,
            'pw_lw_children',
        )
        expected = {
            'data': [
                {
                    'state_name': 'All',
                    'district_name': 'All',
                    'block_name': 'All',
                    'supervisor_name': 'All',
                    'awc_name': 'All',
                    'num_launched_awcs': 22,
                    'valid_visits': 3,
                    'expected_visits': 379,
                    'gm_0_3': 222,
                    'children_0_3': 314,
                    'num_awcs_conducted_cbe': 1,
                    'num_awcs_conducted_vhnd': 8,
                    'thr_21_days': 261,
                    'thr_25_days': 180,
                    'thr_eligible': 598,
                    'home_visits': '0.79 %',
                    'gm': '70.70 %',
                    'cbe': '4.55 %',
                    'thr': '43.65 %'
                },
                {
                    'state_name': 'st1',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'num_launched_awcs': 10,
                    'valid_visits': 3,
                    'expected_visits': 185,
                    'gm_0_3': 83,
                    'children_0_3': 143,
                    'num_awcs_conducted_cbe': 0,
                    'num_awcs_conducted_vhnd': 2,
                    'thr_21_days': 80,
                    'thr_25_days': 24,
                    'thr_eligible': 279,
                    'home_visits': '1.62 %',
                    'gm': '58.04 %',
                    'cbe': '0.00 %',
                    'thr': '28.67 %'
                },
                {
                    'state_name': 'st2',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'num_launched_awcs': 11,
                    'valid_visits': 0,
                    'expected_visits': 193,
                    'gm_0_3': 139,
                    'children_0_3': 171,
                    'num_awcs_conducted_cbe': 1,
                    'num_awcs_conducted_vhnd': 6,
                    'thr_21_days': 181,
                    'thr_25_days': 156,
                    'thr_eligible': 318,
                    'home_visits': '0.00 %',
                    'gm': '81.29 %',
                    'cbe': '9.09 %',
                    'thr': '56.92 %'
                },
                {
                    'state_name': 'st7',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'num_launched_awcs': 1,
                    'valid_visits': 0,
                    'expected_visits': 1,
                    'gm_0_3': 0,
                    'children_0_3': 0,
                    'num_awcs_conducted_cbe': 0,
                    'num_awcs_conducted_vhnd': 0,
                    'thr_21_days': 0,
                    'thr_25_days': 0,
                    'thr_eligible': 1,
                    'home_visits': '0.00 %',
                    'gm': 'Data Not Entered',
                    'cbe': '0.00 %',
                    'thr': '0.00 %'
                }
            ],
            'aggregationLevel': 1,
            'recordsTotal': 3,
            'recordsFiltered': 3
        }
        self.assertDictEqual(expected, data)

    def test_get_service_delivery_data_state_0_3(self):
        data = get_service_delivery_report_data(
            'icds-cas',
            0,
            10,
            'district_name',
            False,
            {
                'aggregation_level': 2,
                'state_id': 'st1',
            },
            2017,
            5,
            'pw_lw_children',
        )
        expected = {
            'data': [
                {
                    'state_name': 'All',
                    'district_name': 'All',
                    'block_name': 'All',
                    'supervisor_name': 'All',
                    'awc_name': 'All',
                    'num_launched_awcs': 10,
                    'valid_visits': 3,
                    'expected_visits': 185,
                    'gm_0_3': 83,
                    'children_0_3': 143,
                    'num_awcs_conducted_cbe': 0,
                    'num_awcs_conducted_vhnd': 2,
                    'thr_21_days': 80,
                    'thr_25_days': 24,
                    'thr_eligible': 279,
                    'home_visits': '1.62 %',
                    'gm': '58.04 %',
                    'cbe': '0.00 %',
                    'thr': '28.67 %'
                },
                {
                    'state_name': 'st1',
                    'district_name': 'd1',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'num_launched_awcs': 10,
                    'valid_visits': 3,
                    'expected_visits': 185,
                    'gm_0_3': 83,
                    'children_0_3': 143,
                    'num_awcs_conducted_cbe': 0,
                    'num_awcs_conducted_vhnd': 2,
                    'thr_21_days': 80,
                    'thr_25_days': 24,
                    'thr_eligible': 279,
                    'home_visits': '1.62 %',
                    'gm': '58.04 %',
                    'cbe': '0.00 %',
                    'thr': '28.67 %'
                }
            ],
            'aggregationLevel': 2,
            'recordsTotal': 1,
            'recordsFiltered': 1
        }
        self.assertDictEqual(expected, data)

    def test_get_service_delivery_data_3_6(self):
        data = get_service_delivery_report_data(
            'icds-cas',
            0,
            10,
            None,
            False,
            {
                'aggregation_level': 1,
            },
            2017,
            5,
            'children',
        )
        expected = {
            'data': [
                {
                    'num_launched_awcs': 22,
                    'state_name': 'All',
                    'district_name': 'All',
                    'block_name': 'All',
                    'supervisor_name': 'All',
                    'awc_name': 'All',
                    'lunch_21_days': 15,
                    'lunch_25_days': 0,
                    'pse_eligible': 991,
                    'pse_21_days': 66,
                    'pse_25_days': 20,
                    'gm_3_5': 473,
                    'children_3_5': 675,
                    'gm': '70.07 %',
                    'pse': '6.66 %',
                    'sn': '1.51 %'
                },
                {
                    'num_launched_awcs': 10,
                    'state_name': 'st1',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'lunch_21_days': 4,
                    'lunch_25_days': 0,
                    'pse_eligible': 483,
                    'pse_21_days': 7,
                    'pse_25_days': 0,
                    'gm_3_5': 234,
                    'children_3_5': 332,
                    'gm': '70.48 %',
                    'pse': '1.45 %',
                    'sn': '0.83 %'
                },
                {
                    'num_launched_awcs': 11,
                    'state_name': 'st2',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'lunch_21_days': 11,
                    'lunch_25_days': 0,
                    'pse_eligible': 507,
                    'pse_21_days': 59,
                    'pse_25_days': 20,
                    'gm_3_5': 239,
                    'children_3_5': 342,
                    'gm': '69.88 %',
                    'pse': '11.64 %',
                    'sn': '2.17 %'
                },
                {
                    'num_launched_awcs': 1,
                    'state_name': 'st7',
                    'district_name': 'Data Not Entered',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'lunch_21_days': 0,
                    'lunch_25_days': 0,
                    'pse_eligible': 1,
                    'pse_21_days': 0,
                    'pse_25_days': 0,
                    'gm_3_5': 0,
                    'children_3_5': 1,
                    'gm': '0.00 %',
                    'pse': '0.00 %',
                    'sn': '0.00 %'
                }
            ],
            'aggregationLevel': 1,
            'recordsTotal': 3,
            'recordsFiltered': 3
        }
        self.assertDictEqual(expected, data)

    def test_get_service_delivery_data_state_3_6(self):
        data = get_service_delivery_report_data(
            'icds-cas',
            0,
            10,
            'district_name',
            False,
            {
                'aggregation_level': 2,
                'state_id': 'st1',
            },
            2017,
            5,
            'children',
        )
        expected = {
            'data': [
                {
                    'num_launched_awcs': 10,
                    'state_name': 'All',
                    'district_name': 'All',
                    'block_name': 'All',
                    'supervisor_name': 'All',
                    'awc_name': 'All',
                    'lunch_21_days': 4,
                    'lunch_25_days': 0,
                    'pse_eligible': 483,
                    'pse_21_days': 7,
                    'pse_25_days': 0,
                    'gm_3_5': 234,
                    'children_3_5': 332,
                    'gm': '70.48 %',
                    'pse': '1.45 %',
                    'sn': '0.83 %'
                },
                {
                    'num_launched_awcs': 10,
                    'state_name': 'st1',
                    'district_name': 'd1',
                    'block_name': 'Data Not Entered',
                    'supervisor_name': 'Data Not Entered',
                    'awc_name': 'Data Not Entered',
                    'lunch_21_days': 4,
                    'lunch_25_days': 0,
                    'pse_eligible': 483,
                    'pse_21_days': 7,
                    'pse_25_days': 0,
                    'gm_3_5': 234,
                    'children_3_5': 332,
                    'gm': '70.48 %',
                    'pse': '1.45 %',
                    'sn': '0.83 %'
                }
            ],
            'aggregationLevel': 2,
            'recordsTotal': 1,
            'recordsFiltered': 1
        }
        self.assertDictEqual(expected, data)
