from django.test.utils import override_settings

from custom.icds_reports.const import ChartColors, MapColors
from custom.icds_reports.messages import underweight_children_help_text
from custom.icds_reports.reports.prevalence_of_undernutrition import get_prevalence_of_undernutrition_data_map, \
    get_prevalence_of_undernutrition_data_chart, get_prevalence_of_undernutrition_sector_data
from django.test import TestCase


@override_settings(SERVER_ENVIRONMENT='icds')
class TestPrevalenceOfUndernutrition(TestCase):

    def test_map_data_keys(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
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
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )['rightLegend']
        self.assertEqual(len(data), 3)
        self.assertIn('info', data)
        self.assertIn('average', data)
        self.assertIn('extended_info', data)

    def test_map_data(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['data'],
            {
                'st1': {'moderately_underweight': 320,
                     'severely_underweight': 40,
                     'normal': 1225,
                     'weighed': 1585,
                     'total': 2375,
                     'original_name': ['st1'],
                     'fillKey': '20%-35%'},
                'st2': {'moderately_underweight': 325,
                     'severely_underweight': 60,
                     'normal': 1505,
                     'weighed': 1890,
                     'total': 2565,
                     'original_name': ['st2'],
                     'fillKey': '20%-35%'},
                'st3': {'moderately_underweight': 0,
                     'severely_underweight': 0,
                     'normal': 0,
                     'weighed': 0,
                     'total': 1,
                     'original_name': ['st3'],
                     'fillKey': '0%-20%'},
                'st4': {'moderately_underweight': 0,
                        'severely_underweight': 0,
                        'normal': 0,
                        'weighed': 0,
                        'total': 2,
                        'original_name': ['st4'],
                        'fillKey': '0%-20%'},
                'st5': {'moderately_underweight': 0,
                        'severely_underweight': 0,
                        'normal': 0,
                        'weighed': 0,
                        'total': 3,
                        'original_name': ['st5'],
                        'fillKey': '0%-20%'},
                'st6': {'moderately_underweight': 0,
                        'severely_underweight': 0,
                        'normal': 0,
                        'weighed': 0,
                        'total': 4,
                        'original_name': ['st6'], 'fillKey': '0%-20%'},
                'st7': {'moderately_underweight': 0,
                        'severely_underweight': 0,
                        'normal': 0,
                        'weighed': 0,
                        'total': 5,
                        'original_name': ['st7'], 'fillKey': '0%-20%'}
            }
        )

    def test_map_data_right_legend_info(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        expected = underweight_children_help_text(age_label="0 - 5 years", html=True)
        self.assertEqual(data['rightLegend']['info'], expected)

    def test_map_data_right_legend_average(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertEqual(data['rightLegend']['average'], '21.44')

    def test_map_data_right_legend_extended_info(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertListEqual(
            data['rightLegend']['extended_info'],
            [
                {'indicator': 'Total Children (0 - 5 years) weighed in given month:', 'value': '3,475'},
                {'indicator': 'Number of children unweighed (0 - 5 years):', 'value': '1,480'},
                {'indicator': '% Severely Underweight (0 - 5 years):', 'value': '2.88%'},
                {'indicator': '% Moderately Underweight (0 - 5 years):', 'value': '18.56%'},
                {'indicator': '% Normal (0 - 5 years):', 'value': '78.56%'}
            ]
        )

    def test_map_data_fills(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['fills'],
            {
                "0%-20%": MapColors.PINK,
                "20%-35%": MapColors.ORANGE,
                "35%-100%": MapColors.RED,
                "defaultFill": MapColors.GREY
            }
        )

    def test_map_data_slug(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertEqual(data['slug'], 'moderately_underweight')

    def test_map_data_label(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
            },
            loc_level='state'
        )
        self.assertEqual(data['label'], 'Percent of Children Underweight (0 - 5 years)')

    def test_map_name_two_locations_represent_by_one_topojson(self):
        data = get_prevalence_of_undernutrition_data_map(
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
                    'severely_underweight': 8,
                    'moderately_underweight': 64,
                    'normal': 245,
                    'weighed': 317,
                    'total': 475,
                    'original_name': ['b1', 'b2'],
                    'fillKey': '20%-35%'
                }
            }
        )

    def test_average_with_two_locations_represent_by_one_topojson(self):
        data = get_prevalence_of_undernutrition_data_map(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'aggregation_level': 3
            },
            loc_level='block',
        )
        self.assertEqual(data['rightLegend']['average'], '22.71')

    def test_chart_data_keys_length(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertEqual(len(data), 5)

    def test_chart_data_location_type(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertEqual(data['location_type'], 'State')

    def test_chart_data_bottom_five(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertListEqual(
            data['bottom_five'],
            [
                {'loc_name': 'st5', 'percent': 0.0},
                {'loc_name': 'st6', 'percent': 0.0},
                {'loc_name': 'st7', 'percent': 0.0},
                {'loc_name': 'st2', 'percent': 20.37037037037037},
                {'loc_name': 'st1', 'percent': 22.71293375394322}
            ]
        )

    def test_chart_data_top_five(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertListEqual(
            data['top_five'],
            [
                {'loc_name': 'st3', 'percent': 0.0},
                {'loc_name': 'st4', 'percent': 0.0},
                {'loc_name': 'st5', 'percent': 0.0},
                {'loc_name': 'st6', 'percent': 0.0},
                {'loc_name': 'st7', 'percent': 0.0}
            ]
        )

    def test_chart_data_elements_length(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertEqual(len(data['chart_data']), 3)

    def test_chart_data_pink(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['chart_data'][0],
            {
                "color": ChartColors.PINK,
                "classed": "dashed",
                "strokeWidth": 2,
                "values": [
                    {
                        "y": 0.0,
                        "x": 1485907200000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.0,
                        "x": 1488326400000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.7478260869565218,
                        "x": 1491004800000,
                        "weighed": 3450,
                        "unweighed": 1655
                    },
                    {
                        "y": 0.7856115107913669,
                        "x": 1493596800000,
                        "weighed": 3475,
                        "unweighed": 1480
                    }
                ],
                "key": "% Normal"
            }
        )

    def test_chart_data_orange(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['chart_data'][1],
            {
                "color": ChartColors.ORANGE,
                "classed": "dashed",
                "strokeWidth": 2,
                "values": [
                    {
                        "y": 0.0,
                        "x": 1485907200000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.0,
                        "x": 1488326400000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.23043478260869565,
                        "x": 1491004800000,
                        "weighed": 3450,
                        "unweighed": 1655
                    },
                    {
                        "y": 0.1856115107913669,
                        "x": 1493596800000,
                        "weighed": 3475,
                        "unweighed": 1480
                    }
                ],
                "key": "% Moderately Underweight (-2 SD)"
            }
        )

    def test_chart_data_red(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertDictEqual(
            data['chart_data'][2],
            {
                "color": ChartColors.RED,
                "classed": "dashed",
                "strokeWidth": 2,
                "values": [
                    {
                        "y": 0.0,
                        "x": 1485907200000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.0,
                        "x": 1488326400000,
                        "weighed": 0,
                        "unweighed": 0
                    },
                    {
                        "y": 0.021739130434782608,
                        "x": 1491004800000,
                        "weighed": 3450,
                        "unweighed": 1655
                    },
                    {
                        "y": 0.02877697841726619,
                        "x": 1493596800000,
                        "weighed": 3475,
                        "unweighed": 1480
                    }
                ],
                "key": "% Severely Underweight (-3 SD) "
            }
        )

    def test_chart_data_all_locations(self):
        data = get_prevalence_of_undernutrition_data_chart(
            'icds-cas',
            config={
                'month': (2017, 5, 1)
            },
            loc_level='state'
        )
        self.assertListEqual(
            data['all_locations'],
            [
                {'loc_name': 'st3', 'percent': 0.0},
                {'loc_name': 'st4', 'percent': 0.0},
                {'loc_name': 'st5', 'percent': 0.0},
                {'loc_name': 'st6', 'percent': 0.0},
                {'loc_name': 'st7', 'percent': 0.0},
                {'loc_name': 'st2', 'percent': 20.37037037037037},
                {'loc_name': 'st1', 'percent': 22.71293375394322}
            ]
        )

    def test_sector_data_keys_length(self):
        data = get_prevalence_of_undernutrition_sector_data(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'block_id': 'b1',
            },
            location_id='b1',
            loc_level='supervisor'
        )
        self.assertEqual(len(data), 3)

    def test_sector_data_info(self):
        data = get_prevalence_of_undernutrition_sector_data(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'block_id': 'b1',
            },
            location_id='b1',
            loc_level='supervisor'
        )
        self.assertEqual(
            data['info'],
            underweight_children_help_text(age_label="0-5 years", html=True)
        )

    def test_sector_data_tooltips_data(self):
        data = get_prevalence_of_undernutrition_sector_data(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'block_id': 'b1',
            },
            location_id='b1',
            loc_level='supervisor'
        )
        self.assertDictEqual(
            data['tooltips_data'],
            {
                "s2": {
                    "weighed": 182,
                    "severely_underweight": 4,
                    "moderately_underweight": 54,
                    "normal": 124,
                    "total": 326
                },
                "s1": {
                    "weighed": 134,
                    "severely_underweight": 8,
                    "moderately_underweight": 36,
                    "normal": 90,
                    "total": 144
                },
                None: {
                    "weighed": 158,
                    "severely_underweight": 6,
                    "moderately_underweight": 45,
                    "normal": 107,
                    "total": 235
                }
            }
        )

    def test_sector_data_chart_data(self):
        data = get_prevalence_of_undernutrition_sector_data(
            'icds-cas',
            config={
                'month': (2017, 5, 1),
                'state_id': 'st1',
                'district_id': 'd1',
                'block_id': 'b1',
            },
            location_id='b1',
            loc_level='supervisor'
        )
        self.assertListEqual(
            data['chart_data'],
            [
                {
                    "color": MapColors.BLUE,
                    "classed": "dashed",
                    "strokeWidth": 2,
                    "values": [
                        [
                            None,
                            0.3227848101265823
                        ],
                        [
                            "s1",
                            0.3283582089552239
                        ],
                        [
                            "s2",
                            0.31868131868131866
                        ]
                    ],
                    "key": ""
                }
            ]
        )
