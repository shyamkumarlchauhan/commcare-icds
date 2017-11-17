from __future__ import absolute_import

import json

import mock
from datetime import datetime

from django.core.serializers.json import DjangoJSONEncoder
from django.test.testcases import TestCase

from custom.icds_reports.sqldata import ChildrenExport, PregnantWomenExport, ExportableMixin, DemographicsExport, \
    SystemUsageExport, AWCInfrastructureExport, BeneficiaryExport


class TestExportData(TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestExportData, cls).setUpClass()
        cls.india_now_mock = mock.patch.object(
            ExportableMixin,
            'india_now',
            new_callable=mock.PropertyMock(return_value='16:21:11 15 November 2017')
        )
        cls.india_now_mock.start()

    @classmethod
    def tearDownClass(cls):
        cls.india_now_mock.stop()
        super(TestExportData, cls).tearDownClass()

    def test_children_export(self):
        self.assertListEqual(
            ChildrenExport(
                config={
                    'domain': 'icds-cas'
                },
            ).get_excel_data('b1'),
            [
                [
                    "Children",
                    [
                        [
                            "State",
                            "Weighing efficiency",
                            "Height Measurement Efficiency",
                            "Total number of unweighed children (0-5 Years)",
                            "Percentage of severely underweight children",
                            "Percentage of moderately underweight children",
                            "Percentage of normal weight-for-age children",
                            "Percentage of children with severe wasting",
                            "Percentage of children with moderate wasting",
                            "Percentage of children with normal weight-for-height",
                            "Percentage of children with severe stunting",
                            "Percentage of children with moderate stunting",
                            "Percentage of children with normal height-for-age",
                            "Percentage of children with completed 1 year immunizations",
                            "Percentage of children breastfed at birth",
                            "Percentage of children exclusively breastfeeding",
                            "Percentage of children initiated complementary feeding (in the past 30 days)",
                            "Percentage of children initiated appropriate complementary feeding",
                            "Percentage of children receiving complementary feeding with adequate diet diversity",
                            "Percentage of children receiving complementary feeding with adequate diet quanity",
                            "Percentage of children receiving complementary feeding with appropriate "
                            "handwashing before feeding"
                        ],
                        [
                            "st1",
                            "67.39 %",
                            "1.42 %",
                            317,
                            "1.75 %",
                            "19.96 %",
                            "71.81 %",
                            "0.00 %",
                            "0.55 %",
                            "0.00 %",
                            "0.66 %",
                            "0.87 %",
                            "0.22 %",
                            "14.77%",
                            "37.50 %",
                            "50.00 %",
                            "65.62 %",
                            "53.52 %",
                            "34.51 %",
                            "39.44 %",
                            "47.89 %"
                        ],
                        [
                            "st1",
                            "67.39 %",
                            "1.42 %",
                            317,
                            "1.75 %",
                            "19.96 %",
                            "71.81 %",
                            "0.00 %",
                            "0.55 %",
                            "0.00 %",
                            "0.66 %",
                            "0.87 %",
                            "0.22 %",
                            "14.77%",
                            "37.50 %",
                            "50.00 %",
                            "65.62 %",
                            "53.52 %",
                            "34.51 %",
                            "39.44 %",
                            "47.89 %"
                        ],
                        [
                            "st1",
                            "67.39 %",
                            "1.42 %",
                            317,
                            "1.75 %",
                            "19.96 %",
                            "71.81 %",
                            "0.00 %",
                            "0.55 %",
                            "0.00 %",
                            "0.66 %",
                            "0.87 %",
                            "0.22 %",
                            "14.77%",
                            "37.50 %",
                            "50.00 %",
                            "65.62 %",
                            "53.52 %",
                            "34.51 %",
                            "39.44 %",
                            "47.89 %"
                        ],
                        [
                            "st1",
                            "67.39 %",
                            "1.42 %",
                            317,
                            "1.75 %",
                            "19.96 %",
                            "71.81 %",
                            "0.00 %",
                            "0.55 %",
                            "0.00 %",
                            "0.66 %",
                            "0.87 %",
                            "0.22 %",
                            "14.77%",
                            "37.50 %",
                            "50.00 %",
                            "65.62 %",
                            "53.52 %",
                            "34.51 %",
                            "39.44 %",
                            "47.89 %"
                        ],
                        [
                            "st1",
                            "67.39 %",
                            "1.42 %",
                            317,
                            "1.75 %",
                            "19.96 %",
                            "71.81 %",
                            "0.00 %",
                            "0.55 %",
                            "0.00 %",
                            "0.66 %",
                            "0.87 %",
                            "0.22 %",
                            "14.77%",
                            "37.50 %",
                            "50.00 %",
                            "65.62 %",
                            "53.52 %",
                            "34.51 %",
                            "39.44 %",
                            "47.89 %"
                        ],
                        [
                            "st2",
                            "70.45 %",
                            "3.04 %",
                            307,
                            "1.92 %",
                            "15.50 %",
                            "66.31 %",
                            "0.41 %",
                            "0.41 %",
                            "0.00 %",
                            "1.82 %",
                            "0.61 %",
                            "1.62 %",
                            "7.19%",
                            "42.86 %",
                            "25.00 %",
                            "60.00 %",
                            "50.81 %",
                            "47.03 %",
                            "33.51 %",
                            "47.57 %"
                        ],
                        [
                            "st2",
                            "70.45 %",
                            "3.04 %",
                            307,
                            "1.92 %",
                            "15.50 %",
                            "66.31 %",
                            "0.41 %",
                            "0.41 %",
                            "0.00 %",
                            "1.82 %",
                            "0.61 %",
                            "1.62 %",
                            "7.19%",
                            "42.86 %",
                            "25.00 %",
                            "60.00 %",
                            "50.81 %",
                            "47.03 %",
                            "33.51 %",
                            "47.57 %"
                        ],
                        [
                            "st2",
                            "70.45 %",
                            "3.04 %",
                            307,
                            "1.92 %",
                            "15.50 %",
                            "66.31 %",
                            "0.41 %",
                            "0.41 %",
                            "0.00 %",
                            "1.82 %",
                            "0.61 %",
                            "1.62 %",
                            "7.19%",
                            "42.86 %",
                            "25.00 %",
                            "60.00 %",
                            "50.81 %",
                            "47.03 %",
                            "33.51 %",
                            "47.57 %"
                        ],
                        [
                            "st2",
                            "70.45 %",
                            "3.04 %",
                            307,
                            "1.92 %",
                            "15.50 %",
                            "66.31 %",
                            "0.41 %",
                            "0.41 %",
                            "0.00 %",
                            "1.82 %",
                            "0.61 %",
                            "1.62 %",
                            "7.19%",
                            "42.86 %",
                            "25.00 %",
                            "60.00 %",
                            "50.81 %",
                            "47.03 %",
                            "33.51 %",
                            "47.57 %"
                        ],
                        [
                            "st2",
                            "70.45 %",
                            "3.04 %",
                            307,
                            "1.92 %",
                            "15.50 %",
                            "66.31 %",
                            "0.41 %",
                            "0.41 %",
                            "0.00 %",
                            "1.82 %",
                            "0.61 %",
                            "1.62 %",
                            "7.19%",
                            "42.86 %",
                            "25.00 %",
                            "60.00 %",
                            "50.81 %",
                            "47.03 %",
                            "33.51 %",
                            "47.57 %"
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Block",
                            "b1"
                        ]
                    ]
                ]
            ]
        )

    def test_pregnant_women_export(self):
        self.assertListEqual(
            PregnantWomenExport(
                config={
                    'domain': 'icds-cas'
                }
            ).get_excel_data('b1'),
            [
                [
                    "Pregnant Women",
                    [
                        [
                            "State",
                            "Number of lactating women",
                            "Number of pregnant women",
                            "Number of postnatal women",
                            "Percentage Anemia",
                            "Percentage Tetanus Completed",
                            "Percent women had at least 1 ANC visit by delivery",
                            "Percent women had at least 2 ANC visit by delivery",
                            "Percent women had at least 3 ANC visit by delivery",
                            "Percent women had at least 4 ANC visit by delivery",
                            "Percentage of women resting during pregnancy",
                            "Percentage of women eating extra meal during pregnancy",
                            "Percentage of trimester 3 women counselled on immediate breastfeeding"
                        ],
                        [
                            "st1",
                            171,
                            120,
                            38,
                            "22.50%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "89.17 %",
                            "90.00 %",
                            "60.32 %"
                        ],
                        [
                            "st1",
                            171,
                            120,
                            38,
                            "22.50%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "89.17 %",
                            "90.00 %",
                            "60.32 %"
                        ],
                        [
                            "st1",
                            171,
                            120,
                            38,
                            "22.50%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "89.17 %",
                            "90.00 %",
                            "60.32 %"
                        ],
                        [
                            "st1",
                            171,
                            120,
                            38,
                            "22.50%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "89.17 %",
                            "90.00 %",
                            "60.32 %"
                        ],
                        [
                            "st1",
                            171,
                            120,
                            38,
                            "22.50%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "89.17 %",
                            "90.00 %",
                            "60.32 %"
                        ],
                        [
                            "st2",
                            154,
                            139,
                            34,
                            "22.30%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "87.05 %",
                            "86.33 %",
                            "57.97 %"
                        ],
                        [
                            "st2",
                            154,
                            139,
                            34,
                            "22.30%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "87.05 %",
                            "86.33 %",
                            "57.97 %"
                        ],
                        [
                            "st2",
                            154,
                            139,
                            34,
                            "22.30%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "87.05 %",
                            "86.33 %",
                            "57.97 %"
                        ],
                        [
                            "st2",
                            154,
                            139,
                            34,
                            "22.30%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "87.05 %",
                            "86.33 %",
                            "57.97 %"
                        ],
                        [
                            "st2",
                            154,
                            139,
                            34,
                            "22.30%",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "0.00 %",
                            "87.05 %",
                            "86.33 %",
                            "57.97 %"
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Block",
                            "b1"
                        ]
                    ]
                ]
            ]
        )

    def test_demographics_export(self):
        self.assertListEqual(
            DemographicsExport(
                config={
                    'domain': 'icds-cas'
                }
            ).get_excel_data('b1'),
            [
                [
                    "Demographics",
                    [
                        [
                            "State",
                            "Number of households",
                            "Total number of beneficiaries (under 6 years old and women between"
                            " 11 and 49 years old, alive and seeking services) who have an aadhaar ID",
                            "Total number of beneficiaries (under 6 years old and women between "
                            "11 and 49 years old, alive and seeking services)",
                            "Percent Aadhaar-seeded beneficaries",
                            "Number of pregnant women",
                            "Number of pregnant women enrolled for services",
                            "Number of lactating women",
                            "Number of lactating women enrolled for services",
                            "Number of children 0-6 years old",
                            "Number of children 0-6 years old enrolled for services",
                            "Number of children 0-6 months old enrolled for services",
                            "Number of children 6 months to 3 years old enrolled for services",
                            "Number of children 3 to 6 years old enrolled for services",
                            "Number of adolescent girls 11 to 14 years old",
                            "Number of adolescent girls 15 to 18 years old",
                            "Number of adolescent girls 11 to 14 years old that are enrolled for services",
                            "Number of adolescent girls 15 to 18 years old that are enrolled for services"
                        ],
                        [
                            "st1",
                            7266,
                            127,
                            437,
                            "29.06 %",
                            120,
                            120,
                            171,
                            171,
                            1227,
                            1227,
                            56,
                            244,
                            927,
                            36,
                            12,
                            36,
                            12
                        ],
                        [
                            "st1",
                            7266,
                            127,
                            437,
                            "29.06 %",
                            120,
                            120,
                            171,
                            171,
                            1227,
                            1227,
                            56,
                            244,
                            927,
                            36,
                            12,
                            36,
                            12
                        ],
                        [
                            "st1",
                            7266,
                            127,
                            437,
                            "29.06 %",
                            120,
                            120,
                            171,
                            171,
                            1227,
                            1227,
                            56,
                            244,
                            927,
                            36,
                            12,
                            36,
                            12
                        ],
                        [
                            "st1",
                            7266,
                            127,
                            437,
                            "29.06 %",
                            120,
                            120,
                            171,
                            171,
                            1227,
                            1227,
                            56,
                            244,
                            927,
                            36,
                            12,
                            36,
                            12
                        ],
                        [
                            "st1",
                            7266,
                            127,
                            437,
                            "29.06 %",
                            120,
                            120,
                            171,
                            171,
                            1227,
                            1227,
                            56,
                            244,
                            927,
                            36,
                            12,
                            36,
                            12
                        ],
                        [
                            "st2",
                            6662,
                            125,
                            547,
                            "22.85 %",
                            139,
                            139,
                            154,
                            154,
                            1322,
                            1322,
                            52,
                            301,
                            969,
                            36,
                            20,
                            36,
                            20
                        ],
                        [
                            "st2",
                            6662,
                            125,
                            547,
                            "22.85 %",
                            139,
                            139,
                            154,
                            154,
                            1322,
                            1322,
                            52,
                            301,
                            969,
                            36,
                            20,
                            36,
                            20
                        ],
                        [
                            "st2",
                            6662,
                            125,
                            547,
                            "22.85 %",
                            139,
                            139,
                            154,
                            154,
                            1322,
                            1322,
                            52,
                            301,
                            969,
                            36,
                            20,
                            36,
                            20
                        ],
                        [
                            "st2",
                            6662,
                            125,
                            547,
                            "22.85 %",
                            139,
                            139,
                            154,
                            154,
                            1322,
                            1322,
                            52,
                            301,
                            969,
                            36,
                            20,
                            36,
                            20
                        ],
                        [
                            "st2",
                            6662,
                            125,
                            547,
                            "22.85 %",
                            139,
                            139,
                            154,
                            154,
                            1322,
                            1322,
                            52,
                            301,
                            969,
                            36,
                            20,
                            36,
                            20
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Block",
                            "b1"
                        ]
                    ]
                ]
            ]
        )

    def test_system_usage_export(self):
        self.assertListEqual(
            SystemUsageExport(
                config={
                    'domain': 'icds-cas'
                }
            ).get_excel_data('b1'),
            [
                [
                    "System Usage",
                    [
                        [
                            "State",
                            "Number of days AWC was open in the given month",
                            "Number of launched AWCs (ever submitted at least one HH reg form)",
                            "Number of household registration forms",
                            "Number of add pregnancy forms",
                            "Number of birth preparedness forms",
                            "Number of delivery forms",
                            "Number of PNC forms",
                            "Number of exclusive breastfeeding forms",
                            "Number of complementary feeding forms",
                            "Number of growth monitoring forms",
                            "Number of take home rations forms",
                            "Number of due list forms"
                        ],
                        [
                            "st1",
                            38,
                            16,
                            85,
                            4,
                            4,
                            1,
                            0,
                            5,
                            12,
                            14,
                            47,
                            5
                        ],
                        [
                            "st1",
                            38,
                            16,
                            85,
                            4,
                            4,
                            1,
                            0,
                            5,
                            12,
                            14,
                            47,
                            5
                        ],
                        [
                            "st1",
                            38,
                            16,
                            85,
                            4,
                            4,
                            1,
                            0,
                            5,
                            12,
                            14,
                            47,
                            5
                        ],
                        [
                            "st1",
                            38,
                            16,
                            85,
                            4,
                            4,
                            1,
                            0,
                            5,
                            12,
                            14,
                            47,
                            5
                        ],
                        [
                            "st1",
                            38,
                            16,
                            85,
                            4,
                            4,
                            1,
                            0,
                            5,
                            12,
                            14,
                            47,
                            5
                        ],
                        [
                            "st2",
                            34,
                            22,
                            79,
                            4,
                            4,
                            2,
                            2,
                            5,
                            4,
                            20,
                            65,
                            17
                        ],
                        [
                            "st2",
                            34,
                            22,
                            79,
                            4,
                            4,
                            2,
                            2,
                            5,
                            4,
                            20,
                            65,
                            17
                        ],
                        [
                            "st2",
                            34,
                            22,
                            79,
                            4,
                            4,
                            2,
                            2,
                            5,
                            4,
                            20,
                            65,
                            17
                        ],
                        [
                            "st2",
                            34,
                            22,
                            79,
                            4,
                            4,
                            2,
                            2,
                            5,
                            4,
                            20,
                            65,
                            17
                        ],
                        [
                            "st2",
                            34,
                            22,
                            79,
                            4,
                            4,
                            2,
                            2,
                            5,
                            4,
                            20,
                            65,
                            17
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Block",
                            "b1"
                        ]
                    ]
                ]
            ]
        )

    def test_awc_infrastructure_export(self):
        self.assertListEqual(
            AWCInfrastructureExport(
                config={
                    'domain': 'icds-cas'
                }
            ).get_excel_data('b1'),
            [
                [
                    "AWC Infrastructure",
                    [
                        [
                            "State",
                            "Percentage AWCs with drinking water",
                            "Percentage AWCs with functional toilet",
                            "Percentage AWCs with medicine kit",
                            "Percentage AWCs with weighing scale: infants",
                            "Percentage AWCs with weighing scale: mother and child"
                        ],
                        [
                            "st1",
                            "50.00 %",
                            "25.00 %",
                            "30.77 %",
                            "38.46 %",
                            "13.46 %"
                        ],
                        [
                            "st1",
                            "50.00 %",
                            "25.00 %",
                            "30.77 %",
                            "38.46 %",
                            "13.46 %"
                        ],
                        [
                            "st1",
                            "50.00 %",
                            "25.00 %",
                            "30.77 %",
                            "38.46 %",
                            "13.46 %"
                        ],
                        [
                            "st1",
                            "50.00 %",
                            "25.00 %",
                            "30.77 %",
                            "38.46 %",
                            "13.46 %"
                        ],
                        [
                            "st1",
                            "50.00 %",
                            "25.00 %",
                            "30.77 %",
                            "38.46 %",
                            "13.46 %"
                        ],
                        [
                            "st2",
                            "35.42 %",
                            "20.83 %",
                            "31.25 %",
                            "29.17 %",
                            "10.42 %"
                        ],
                        [
                            "st2",
                            "35.42 %",
                            "20.83 %",
                            "31.25 %",
                            "29.17 %",
                            "10.42 %"
                        ],
                        [
                            "st2",
                            "35.42 %",
                            "20.83 %",
                            "31.25 %",
                            "29.17 %",
                            "10.42 %"
                        ],
                        [
                            "st2",
                            "35.42 %",
                            "20.83 %",
                            "31.25 %",
                            "29.17 %",
                            "10.42 %"
                        ],
                        [
                            "st2",
                            "35.42 %",
                            "20.83 %",
                            "31.25 %",
                            "29.17 %",
                            "10.42 %"
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Block",
                            "b1"
                        ]
                    ]
                ]
            ]
        )

    def test_beneficiary_export(self):
        self.assertJSONEqual(
            json.dumps(
                BeneficiaryExport(
                    config={
                        'domain': 'icds-cas',
                        'month': datetime(2017, 5, 1),
                        'awc_id': 'a7'
                    },
                    loc_level=5
                ).get_excel_data('a7'),
                cls=DjangoJSONEncoder
            ),
            json.dumps([
                [
                    "Child Beneficiary",
                    [
                        [
                            "Name",
                            "Date of Birth",
                            "Current Age (In years)",
                            "Sex ",
                            "1 Year Immunizations Complete",
                            "Month for data shown",
                            "Weight recorded",
                            "Height recorded",
                            "Weight-for-Age Status",
                            "Weight-for-Height Status",
                            "Height-for-Age status",
                            "PSE Attendance (Days)"
                        ],
                        [
                            "Name 1783",
                            "2013-06-06",
                            "3 years 10 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.80",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            23
                        ],
                        [
                            "Name 1788",
                            "2012-12-03",
                            "4 years 4 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.10",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            19
                        ],
                        [
                            "Name 1790",
                            "2012-12-15",
                            "4 years 4 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            20
                        ],
                        [
                            "Name 1794",
                            "2012-04-03",
                            "5 years ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            25
                        ],
                        [
                            "Name 1795",
                            "2014-01-20",
                            "3 years 3 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "11.30",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            17
                        ],
                        [
                            "Name 1797",
                            "2012-05-12",
                            "4 years 11 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "15.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            23
                        ],
                        [
                            "Name 1801",
                            "2011-12-27",
                            "5 years 4 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            19
                        ],
                        [
                            "Name 1807",
                            "2011-10-16",
                            "5 years 6 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            23
                        ],
                        [
                            "Name 1811",
                            "2011-11-29",
                            "5 years 5 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            21
                        ],
                        [
                            "Name 1812",
                            "2012-02-06",
                            "5 years 2 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            17
                        ],
                        [
                            "Name 1814",
                            "2017-01-28",
                            "3 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None
                        ],
                        [
                            "Name 1832",
                            "2015-09-14",
                            "1 year 7 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "10.60",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 1876",
                            "2016-01-11",
                            "1 year 3 months ",
                            "M",
                            "Yes",
                            "2017-05-01",
                            "8.80",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2027",
                            "2016-12-15",
                            "4 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            "Data Not Entered",
                            None,
                            None,
                            None
                        ],
                        [
                            "Name 2054",
                            "2016-05-26",
                            "11 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "8.60",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2056",
                            "2014-11-29",
                            "2 years 5 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "11.40",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2060",
                            "2015-10-10",
                            "1 year 6 months ",
                            "M",
                            "Yes",
                            "2017-05-01",
                            "8.80",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2073",
                            "2015-08-10",
                            "1 year 8 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "9.50",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2094",
                            "2014-12-04",
                            "2 years 4 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "10.50",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2117",
                            "2015-11-18",
                            "1 year 5 months ",
                            "M",
                            "Yes",
                            "2017-05-01",
                            "9.80",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2134",
                            "2015-12-12",
                            "1 year 4 months ",
                            "M",
                            "Yes",
                            "2017-05-01",
                            "7.90",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2141",
                            "2015-03-05",
                            "2 years 1 month ",
                            "M",
                            "No",
                            "2017-05-01",
                            "10.00",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2171",
                            "2016-08-27",
                            "8 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "7.50",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2173",
                            "2015-05-24",
                            "1 year 11 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "9.40",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2182",
                            "2014-12-12",
                            "2 years 4 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.50",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2188",
                            "2014-08-16",
                            "2 years 8 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.40",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2192",
                            "2015-10-07",
                            "1 year 6 months ",
                            "F",
                            "Yes",
                            "2017-05-01",
                            "8.00",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2207",
                            "2016-01-21",
                            "1 year 3 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "8.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2210",
                            "2015-05-18",
                            "1 year 11 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "9.60",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2241",
                            "2012-10-14",
                            "4 years 6 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.00",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            21
                        ],
                        [
                            "Name 2250",
                            "2014-06-10",
                            "2 years 10 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2254",
                            "2013-01-28",
                            "4 years 3 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "14.00",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            18
                        ],
                        [
                            "Name 2263",
                            "2016-09-08",
                            "7 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "8.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2265",
                            "2014-02-16",
                            "3 years 2 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.80",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            18
                        ],
                        [
                            "Name 2266",
                            "2014-03-13",
                            "3 years 1 month ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.60",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            22
                        ],
                        [
                            "Name 2267",
                            "2012-12-25",
                            "4 years 4 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "13.60",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            20
                        ],
                        [
                            "Name 2271",
                            "2013-05-13",
                            "3 years 11 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.20",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            24
                        ],
                        [
                            "Name 2275",
                            "2011-09-27",
                            "5 years 7 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            22
                        ],
                        [
                            "Name 2276",
                            "2012-07-22",
                            "4 years 9 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.80",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            22
                        ],
                        [
                            "Name 2330",
                            "2013-06-29",
                            "3 years 10 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.50",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            21
                        ],
                        [
                            "Name 2331",
                            "2013-05-09",
                            "3 years 11 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.40",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            19
                        ],
                        [
                            "Name 2332",
                            "2012-02-20",
                            "5 years 2 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            25
                        ],
                        [
                            "Name 2333",
                            "2014-06-05",
                            "2 years 10 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "10.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            None
                        ],
                        [
                            "Name 2335",
                            "2013-10-14",
                            "3 years 6 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "11.90",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            20
                        ],
                        [
                            "Name 2337",
                            "2013-12-04",
                            "3 years 4 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.60",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            26
                        ],
                        [
                            "Name 2338",
                            "2013-07-03",
                            "3 years 9 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "14.20",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            24
                        ],
                        [
                            "Name 2339",
                            "2013-11-29",
                            "3 years 5 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.90",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            27
                        ],
                        [
                            "Name 2340",
                            "2013-07-25",
                            "3 years 9 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.10",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            17
                        ],
                        [
                            "Name 2341",
                            "2012-08-07",
                            "4 years 8 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "14.20",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            24
                        ],
                        [
                            "Name 2342",
                            "2013-09-24",
                            "3 years 7 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "11.00",
                            None,
                            "Moderately underweight",
                            "Data Not Entered",
                            "Data Not Entered",
                            24
                        ],
                        [
                            "Name 2343",
                            "2011-07-30",
                            "5 years 9 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            23
                        ],
                        [
                            "Name 2344",
                            "2013-03-09",
                            "4 years 1 month ",
                            "M",
                            "No",
                            "2017-05-01",
                            "13.40",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            22
                        ],
                        [
                            "Name 2345",
                            "2011-08-20",
                            "5 years 8 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            17
                        ],
                        [
                            "Name 2346",
                            "2014-01-20",
                            "3 years 3 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            "12.70",
                            None,
                            "Normal weight for age",
                            "Data Not Entered",
                            "Data Not Entered",
                            16
                        ],
                        [
                            "Name 2347",
                            "2011-11-21",
                            "5 years 5 months ",
                            "F",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            25
                        ],
                        [
                            "Name 2531",
                            "2011-10-16",
                            "5 years 6 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            19
                        ],
                        [
                            "Name 2534",
                            "2011-10-20",
                            "5 years 6 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            22
                        ],
                        [
                            "Name 4377",
                            "2011-06-22",
                            "5 years 10 months ",
                            "M",
                            "No",
                            "2017-05-01",
                            None,
                            None,
                            None,
                            None,
                            None,
                            18
                        ]
                    ]
                ],
                [
                    "Filters",
                    [
                        [
                            "Generated at",
                            "16:21:11 15 November 2017"
                        ],
                        [
                            "Awc",
                            "a7"
                        ],
                        [
                            "Month",
                            "May"
                        ],
                        [
                            "Year",
                            2017
                        ]
                    ]
                ]
            ])
        )
