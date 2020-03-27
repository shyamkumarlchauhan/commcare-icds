from django.test import TestCase

from custom.icds_reports.models.views import KPIAPIView
from datetime import date


class KPIAPITest(TestCase):

    def test_view_content(self):
        data = KPIAPIView.objects.filter(
            month=date(2017, 5, 1)
        )
        first_result = data[0]
        self.assertDictEqual(
            {
                "state_id": "st1", "state_name": "st1", "state_site_code": "st1",
                "district_id": "d1", "district_name": "d1", "district_site_code": "d1",
                "aggregation_level": 2, "district_map_location_name": "", "state_map_location_name": "",
                "month": date(2017, 5, 1), "cbe_conducted": 1, "vhnd_conducted": 3, "num_launched_awcs": 10,
                "wer_weighed": 475, "wer_eligible": 317, "bf_at_birth": 1, "born_in_month": 2,
                "cf_initiation_eligible": 14, "cf_initiation_in_month": 17
             },
            first_result
        )
