from __future__ import absolute_import

from django.test import TestCase

from custom.icds_reports.reports.demographics_data import get_demographics_data


class TestDemographics(TestCase):

    def test_data(self):
        self.assertDictEqual(
            get_demographics_data(
                'icds-cas',
                (2017, 5, 28),
                {
                    'month': (2017, 5, 1),
                    'prev_month': (2017, 4, 1),
                    'aggregation_level': 1
                }
            ),
            {
                "records": [
                    [
                        {
                            "redirect": "registered_household",
                            "all": None,
                            "format": "number",
                            "color": "red",
                            "percent": 0.0,
                            "value": 6964,
                            "label": "Registered Households",
                            "frequency": "month",
                            "help_text": "Total number of households registered"
                        },
                        {
                            "redirect": "adhaar",
                            "all": 500,
                            "format": "percent_and_div",
                            "color": "green",
                            "percent": 4.800000000000011,
                            "value": 131,
                            "label": "Percent Aadhaar-seeded Beneficiaries",
                            "frequency": "month",
                            "help_text": "Percentage of ICDS beneficiaries"
                                         " whose Aadhaar identification has been captured"
                        }
                    ],
                    [
                        {
                            "all": 1287,
                            "format": "percent_and_div",
                            "color": "red",
                            "percent": 0.0,
                            "value": 1287,
                            "label": "Percent children (0-6 years) enrolled for ICDS services",
                            "frequency": "month",
                            "help_text": "Percentage of children registered"
                                         " between 0-6 years old who are enrolled for ICDS services"
                        },
                        {
                            "redirect": "enrolled_women",
                            "all": 155,
                            "format": "percent_and_div",
                            "color": "red",
                            "percent": 0.0,
                            "value": 155,
                            "label": "Percent pregnant women enrolled for ICDS services",
                            "frequency": "month",
                            "help_text": "Percentage of pregnant women "
                                         "registered who are enrolled for ICDS services"
                        }
                    ],
                    [
                        {
                            "all": 166,
                            "format": "percent_and_div",
                            "color": "red",
                            "percent": 0.0,
                            "value": 166,
                            "label": "Percent lactating women enrolled for ICDS services",
                            "frequency": "month",
                            "help_text": "Percentage of lactating women "
                                         "registered who are enrolled for ICDS services"
                        },
                        {
                            "redirect": "adolescent_girls",
                            "all": 47,
                            "format": "percent_and_div",
                            "color": "red",
                            "percent": 0.0,
                            "value": 47,
                            "label": "Percent adolescent girls (11-18 years) enrolled for ICDS services",
                            "frequency": "month",
                            "help_text": "Percentage of adolescent girls registered"
                                         " between 11-18 years old who are enrolled for ICDS services"
                        }
                    ]
                ]
            }
        )
