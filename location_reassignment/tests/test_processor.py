from collections import namedtuple
from unittest import TestCase

from mock import call, patch

from custom.icds.location_reassignment.const import MOVE_OPERATION
from custom.icds.location_reassignment.exceptions import InvalidTransitionError
from custom.icds.location_reassignment.processor import Processor

LocationType = namedtuple("LocationType", ["code"])
Location = namedtuple("Location", ["site_code"])


class TestProcessor(TestCase):
    domain = "test"

    @patch('custom.icds.location_reassignment.processor.deprecate_locations')
    @patch('corehq.apps.locations.models.SQLLocation.active_objects.filter')
    @patch('corehq.apps.locations.models.LocationType.objects.by_domain')
    def test_process(self, location_types_mock, locations_mock, deprecate_locations_mock):
        type_codes = ['state', 'supervisor', 'awc']
        site_codes = ['131', '112', '13', '12']
        location_types_mock.return_value = list(map(lambda site_code: LocationType(code=site_code), type_codes))
        location_131 = Location(site_code='131')
        location_112 = Location(site_code='112')
        location_13 = Location(site_code='13')
        location_12 = Location(site_code='12')
        locations = [location_12, location_13, location_112, location_131]
        locations_mock.return_value = locations
        deprecate_locations_mock.return_value = []
        transitions = {
            'awc': {
                'Move': {'131': '112'},
            },
            'supervisor': {
                'Move': {'13': '12'}
            },
            'state': {}
        }
        Processor(self.domain, transitions, site_codes).process()
        calls = [call([location_112], [location_131], MOVE_OPERATION),
                 call([location_12], [location_13], MOVE_OPERATION)]
        deprecate_locations_mock.assert_has_calls(calls)
        self.assertEqual(deprecate_locations_mock.call_count, 2)

    @patch('corehq.apps.locations.models.SQLLocation.active_objects.filter')
    @patch('corehq.apps.locations.models.LocationType.objects.by_domain')
    def test_missing_locations(self, location_types_mock, locations_mock):
        type_codes = ['awc']
        site_codes = ['131', '112']
        location_types_mock.return_value = list(map(lambda site_code: LocationType(code=site_code), type_codes))
        location_131 = Location(site_code='131')
        locations_mock.return_value = [location_131]
        transitions = {
            'awc': {
                'Move': {'131': '112'},
            }
        }
        with self.assertRaises(InvalidTransitionError) as e:
            Processor(self.domain, transitions, site_codes).process()
            self.assertEqual(str(e.exception), "Could not load location with following site codes: 112")
