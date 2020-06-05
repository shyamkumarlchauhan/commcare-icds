from django.test.utils import override_settings

from custom.icds_reports.const import ChartColors, MapColors
from custom.icds_reports.reports.immunization_coverage_data import get_immunization_coverage_data_map, \
    get_immunization_coverage_data_chart, get_immunization_coverage_sector_data
from django.test import TestCase


@override_settings(SERVER_ENVIRONMENT='icds')
class TestImmunizationCoverage(TestCase):

    def test_map_data_keys(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertEqual(len(data), 5)
        self.assertIn('rightLegend', data)
        self.assertIn('fills', data)
        self.assertIn('data', data)
        self.assertIn('slug', data)
        self.assertIn('label', data)

    def test_map_data_right_legend_keys(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )['rightLegend']
        self.assertEqual(len(data), 3)
        self.assertIn('info', data)
        self.assertIn('average', data)
        self.assertIn('extended_info', data)

    def test_map_data(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['data'],
            {
                'st4': {'all': 0, 'original_name': ['st4'], 'children': 0, 'fillKey': '0%-20%'},
                'st5': {'all': 0, 'original_name': ['st5'], 'children': 0, 'fillKey': '0%-20%'},
                'st6': {'all': 0, 'original_name': ['st6'], 'children': 0, 'fillKey': '0%-20%'},
                'st7': {'all': 1, 'original_name': ['st7'], 'children': 0, 'fillKey': '0%-20%'},
                'st1': {'all': 568, 'original_name': ['st1'], 'children': 83, 'fillKey': '0%-20%'},
                'st2': {'all': 617, 'original_name': ['st2'], 'children': 45, 'fillKey': '0%-20%'},
                'st3': {'all': 0, 'original_name': ['st3'], 'children': 0, 'fillKey': '0%-20%'}
            }
        )

    def test_map_data_with_age_1_2_ff(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state',
            icds_features_flag=True
        )
        self.assertDictEqual(
            data['data'],
            {
                'st1': {'all': 44, 'original_name': ['st1'], 'children': 4, 'fillKey': '0%-20%'},
                'st2': {'all': 57, 'original_name': ['st2'], 'children': 6, 'fillKey': '0%-20%'}
            }
        )

    def test_map_data_right_legend_info(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        expected = (
            "Of the total number of children enrolled for Anganwadi Services who are over a year old, "
            "the percentage of children who have received the complete immunization as per the National "
            "Immunization Schedule of India that is required by age 1."
            "<br/><br/>"
            "This includes the following immunizations:<br/>"
            "If Pentavalent path: Penta1/2/3, OPV1/2/3, BCG, Measles, VitA1<br/>"
            "If DPT/HepB path: DPT1/2/3, HepB1/2/3, OPV1/2/3, BCG, Measles, VitA1"
        )
        self.assertEqual(data['rightLegend']['info'], expected)

    def test_map_data_right_legend_average(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertEqual(data['rightLegend']['average'], 10.79258010118044)

    def test_map_data_right_legend_extended_info(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertListEqual(
            data['rightLegend']['extended_info'],
            [
                {
                    'indicator': 'Total number of ICDS Child beneficiaries older than 1 year:',
                    'value': "1,186"
                },
                {
                    'indicator': (
                        'Total number of children who have recieved '
                        'complete immunizations required by age 1:'
                    ),
                    'value': "128"
                },
                {
                    'indicator': (
                        '% of children who have recieved complete immunizations required by age 1:'
                    ),
                    'value': '10.79%'
                }
            ]
        )

    def test_map_data_right_legend_extended_info_with_age_1_2_ff(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state',
            icds_features_flag=True
        )
        self.assertListEqual(
            data['rightLegend']['extended_info'],
            [
                {
                    'indicator': 'Total number of ICDS Child beneficiaries between 1-2 years old:',
                    'value': "101"
                },
                {
                    'indicator': (
                        'Total number of children between 1-2 years old who have recieved '
                        'complete immunizations required by age 1:'
                    ),
                    'value': "10"
                },
                {
                    'indicator': (
                        '% of children between 1-2 years old who have recieved complete'
                        ' immunizations required by age 1:'
                    ),
                    'value': '9.90%'
                }
            ]
        )

    def test_map_data_fills(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['fills'],
            {
                "0%-20%": MapColors.RED,
                "20%-60%": MapColors.ORANGE,
                "60%-100%": MapColors.PINK,
                "defaultFill": MapColors.GREY
            }
        )

    def test_map_data_slug(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertEqual(data['slug'], 'institutional_deliveries')

    def test_map_data_label(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'aggregation_level': 1
            },
            loc_level='state'
        )
        self.assertEqual(data['label'], 'Percent Immunization Coverage at 1 year')

    def test_map_name_two_locations_represent_by_one_topojson(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'aggregation_level': 3
            },
            loc_level='block',
        )
        self.assertDictEqual(
            data['data'],
            {
                'block_map': {
                    'all': 568,
                    'original_name': ['b1', 'b2'],
                    'children': 83,
                    'fillKey': '0%-20%'
                }
            }
        )

    def test_average_with_two_locations_represent_by_one_topojson(self):
        data = get_immunization_coverage_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'aggregation_level': 3
            },
            loc_level='block',
        )
        self.assertEqual(data['rightLegend']['average'], 14.612676056338028)

    def test_chart_data(self):
        self.assertDictEqual(
            get_immunization_coverage_data_chart(
                'icds-cas',
                config={
                    'month': (2017, 5, 1),
                    'aggregation_level': 1
                },
                loc_level='state'
            ),
            {
                "location_type": "State",
                "bottom_five": [
                    {'loc_name': 'st3', 'percent': 0.0},
                    {'loc_name': 'st4', 'percent': 0.0},
                    {'loc_name': 'st5', 'percent': 0.0},
                    {'loc_name': 'st6', 'percent': 0.0},
                    {'loc_name': 'st7', 'percent': 0.0},
                ],
                "top_five": [
                    {'loc_name': 'st1', 'percent': 14.612676056338028},
                    {'loc_name': 'st2', 'percent': 7.293354943273906},
                    {'loc_name': 'st3', 'percent': 0.0},
                    {'loc_name': 'st4', 'percent': 0.0},
                    {'loc_name': 'st5', 'percent': 0.0},
                ],
                "chart_data": [
                    {
                        "color": ChartColors.BLUE,
                        "classed": "dashed",
                        "strokeWidth": 2,
                        "values": [
                            {
                                "y": 0,
                                "x": 1485907200000,
                                "all": 0,
                                "in_month": 0
                            },
                            {
                                "y": 0,
                                "x": 1488326400000,
                                "all": 0,
                                "in_month": 0
                            },
                            {
                                "y": 0.10526315789473684,
                                "x": 1491004800000,
                                "all": 1159,
                                "in_month": 122
                            },
                            {
                                "y": 0.10792580101180438,
                                "x": 1493596800000,
                                "all": 1186,
                                "in_month": 128
                            }
                        ],
                        "key": "% Children received complete immunizations by 1 year"
                    }
                ],
                "all_locations": [
                    {'loc_name': 'st1', 'percent': 14.612676056338028},
                    {'loc_name': 'st2', 'percent': 7.293354943273906},
                    {'loc_name': 'st3', 'percent': 0.0},
                    {'loc_name': 'st4', 'percent': 0.0},
                    {'loc_name': 'st5', 'percent': 0.0},
                    {'loc_name': 'st6', 'percent': 0.0},
                    {'loc_name': 'st7', 'percent': 0.0},
                ]
            }
        )

    def test_chart_data_with_age_1_2_ff(self):
        self.assertDictEqual(
            get_immunization_coverage_data_chart(
                'icds-cas',
                config={
                    'month': (2017, 5, 1),
                    'aggregation_level': 1
                },
                loc_level='state',
                icds_features_flag=True
            ),
            {
                'chart_data': [
                    {
                        'values': [
                            {
                                'x': 1485907200000,
                                'y': 0,
                                'all': 0,
                                'in_month': 0
                            },
                            {
                                'x': 1488326400000,
                                'y': 0,
                                'all': 0,
                                'in_month': 0
                            },
                            {
                                'x': 1491004800000,
                                'y': 0.0784313725490196,
                                'all': 102,
                                'in_month': 8
                            },
                            {
                                'x': 1493596800000,
                                'y': 0.09900990099009901,
                                'all': 101,
                                'in_month': 10
                            }
                        ],
                        'key': '% Children between 1-2 years old who received complete immunizations by 1 year',
                        'strokeWidth': 2,
                        'classed': 'dashed',
                        'color': '#005ebd'
                    }
                ],
                'all_locations': [
                    {'loc_name': 'st2', 'percent': 10.526315789473685},
                    {'loc_name': 'st1', 'percent': 9.090909090909092}
                ],
                'top_five': [
                    {'loc_name': 'st2', 'percent': 10.526315789473685},
                    {'loc_name': 'st1', 'percent': 9.090909090909092}
                ],
                'bottom_five': [
                    {'loc_name': 'st2', 'percent': 10.526315789473685},
                    {'loc_name': 'st1', 'percent': 9.090909090909092
                    }
                ],
                'location_type': 'State'
            }
        )

    def test_sector_data(self):
        self.assertDictEqual(
            get_immunization_coverage_sector_data(
                'icds-cas',
                config={
                    'month': (2017, 5, 1),
                    'state_id': 'st1',
                    'district_id': 'd1',
                    'block_id': 'b1',
                    'aggregation_level': 4
                },
                location_id='b1',
                loc_level='supervisor'
            ),
            {
                "info": "Of the total number of children enrolled for Anganwadi Services who are over a year old, "
                        "the percentage of children who have received the complete immunization as per the "
                        "National Immunization Schedule of India that is required by age 1."
                        "<br/><br/>"
                        "This includes the following immunizations:<br/>"
                        "If Pentavalent path: Penta1/2/3, OPV1/2/3, BCG, Measles, VitA1<br/>"
                        "If DPT/HepB path: DPT1/2/3, HepB1/2/3, OPV1/2/3, BCG, Measles, VitA1",
                "tooltips_data": {
                    "s2": {
                        "all": 193,
                        "children": 3
                    },
                    "s1": {
                        "all": 99,
                        "children": 31
                    }
                },
                "chart_data": [
                    {
                        "color": MapColors.BLUE,
                        "classed": "dashed",
                        "strokeWidth": 2,
                        "values": [
                            [
                                "s1",
                                0.31313131313131315
                            ],
                            [
                                "s2",
                                0.015544041450777202
                            ]
                        ],
                        "key": ""
                    }
                ]
            }

        )

    def test_sector_data_with_age_1_2_ff(self):
        self.assertDictEqual(
            get_immunization_coverage_sector_data(
                'icds-cas',
                config={
                    'month': (2017, 5, 1),
                    'state_id': 'st1',
                    'district_id': 'd1',
                    'block_id': 'b1',
                    'aggregation_level': 4
                },
                location_id='b1',
                loc_level='supervisor',
                icds_features_flag=True
            ),
            {
                "info": "Of the total number of children enrolled for Anganwadi Services who are between"
                        " 1-2 years old, "
                        "the percentage of children who have received the complete immunization as per the "
                        "National Immunization Schedule of India that is required by age 1."
                        "<br/><br/>"
                        "This includes the following immunizations:<br/>"
                        "If Pentavalent path: Penta1/2/3, OPV1/2/3, BCG, Measles, VitA1<br/>"
                        "If DPT/HepB path: DPT1/2/3, HepB1/2/3, OPV1/2/3, BCG, Measles, VitA1",
                "tooltips_data": {
                    "s2": {
                        "all": 12,
                        "children": 3
                    },
                    "s1": {
                        "all": 1,
                        "children": 0
                    }
                },
                "chart_data": [
                    {
                        "color": MapColors.BLUE,
                        "classed": "dashed",
                        "strokeWidth": 2,
                        "values": [
                            [
                                "s1",
                                0.0
                            ],
                            [
                                "s2",
                                0.25
                            ]
                        ],
                        "key": ""
                    }
                ]
            }
        )
