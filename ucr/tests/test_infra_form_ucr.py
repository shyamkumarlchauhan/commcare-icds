from __future__ import absolute_import
from __future__ import unicode_literals

from custom.icds_reports.ucr.tests.test_base_form_ucr import BaseFormsTest


class TestInfraForms(BaseFormsTest):
    ucr_name = "static-icds-cas-static-infrastructure_form"

    def test_infra_form_v10326(self):
        self._test_data_source_results(
            'infrastructure_details_v10326',
            [{
                "doc_id": None,
                "submitted_on": None,
                "month": None,
                "where_housed": None,
                "provided_building": None,
                "other_building": None,
                "awc_building": None,
                "access_physically_challenged": '',
                "toilet_facility": "2",
                "type_toilet": None,
                "source_drinking_water": "2",
                "kitchen": '',
                "space_storing_supplies": '',
                "adequate_space_pse": '',
                "space_pse": None,
                "medicine_kits_available": 0,
                "preschool_kit_available": None,
                "baby_scale_available": 0,
                "flat_scale_available": 0,
                "adult_scale_available": 1,
                "cooking_utensils_available": 0,
                "iec_bcc_available": 0,
                "nhed_kit_available": 0,
                "referral_slip_available": 0,
                "plates_available": 0,
                "tumblers_available": 0,
                "measure_cups_available": 0,
                "food_storage_available": 0,
                "water_storage_available": 0,
                "chair_available": 0,
                "table_available": 0,
                "mats_available": 0,
                "medicine_kits_usable": 0,
                "preschool_kit_usable": None,
                "baby_scale_usable": 0,
                "flat_scale_usable": 0,
                "adult_scale_usable": 0,
                "cooking_utensils_usable": 0,
                "iec_bcc_usable": 0,
                "nhed_kit_usable": 0,
                "referral_slip_usable": 0,
                "plates_usable": 0,
                "tumblers_usable": 0,
                "measure_cups_usable": 0,
                "food_storage_usable": 0,
                "water_storage_usable": 0,
                "chair_usable": 0,
                "table_usable": 0,
                "mats_usable": 0,
                "use_salt": 0,
                "type_of_building": None,
                "type_of_building_pucca": 0,
                "type_of_building_semi_pucca": 0,
                "type_of_building_kuccha": 0,
                "type_of_building_partial_covered_space": 0,
                "clean_water": 1,
                "functional_toilet": 0,
                "has_adequate_space_pse": 0,
                "electricity_awc": None,
                "infantometer": None,
                "stadiometer": None,
            }])

    def test_infra_form_v10475(self):
        self._test_data_source_results(
            'infrastructure_details_v10475',
            [{
                "doc_id": None,
                "submitted_on": None,
                "month": None,
                "where_housed": None,
                "provided_building": None,
                "other_building": None,
                "awc_building": None,
                "access_physically_challenged": '1',
                "toilet_facility": '1',
                "type_toilet": '1',
                "source_drinking_water": '2',
                "kitchen": '1',
                "space_storing_supplies": '1',
                "adequate_space_pse": '1',
                "space_pse": '1',
                "medicine_kits_available": 1,
                "preschool_kit_available": 1,
                "baby_scale_available": 0,
                "flat_scale_available": 1,
                "adult_scale_available": 1,
                "cooking_utensils_available": 1,
                "iec_bcc_available": 0,
                "nhed_kit_available": 0,
                "referral_slip_available": 1,
                "plates_available": 1,
                "tumblers_available": 1,
                "measure_cups_available": 0,
                "food_storage_available": 1,
                "water_storage_available": 1,
                "chair_available": 1,
                "table_available": 1,
                "mats_available": 1,
                "medicine_kits_usable": 1,
                "preschool_kit_usable": 1,
                "baby_scale_usable": 0,
                "flat_scale_usable": 0,
                "adult_scale_usable": 1,
                "cooking_utensils_usable": 1,
                "iec_bcc_usable": 0,
                "nhed_kit_usable": 0,
                "referral_slip_usable": 1,
                "plates_usable": 1,
                "tumblers_usable": 1,
                "measure_cups_usable": 0,
                "food_storage_usable": 1,
                "water_storage_usable": 1,
                "chair_usable": 1,
                "table_usable": 1,
                "mats_usable": 1,
                "use_salt": 1,
                "type_of_building": None,
                "type_of_building_pucca": 0,
                "type_of_building_semi_pucca": 0,
                "type_of_building_kuccha": 0,
                "type_of_building_partial_covered_space": 0,
                "clean_water": 1,
                "functional_toilet": 1,
                "has_adequate_space_pse": 1,
                "electricity_awc": 1,
                "infantometer": 1,
                "stadiometer": 1,
            }])
