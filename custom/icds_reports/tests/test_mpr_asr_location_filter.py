from django.test.testcases import TestCase

from corehq.apps.domain.shortcuts import create_domain
from corehq.apps.locations.models import LocationType, SQLLocation
from corehq.apps.users.models import CommCareUser, UserRole, Permissions
from custom.icds_reports.filters import load_restricted_locs
import mock


class TestMprAsrLocationFilter(TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestMprAsrLocationFilter, cls).setUpClass()
        cls.domain = create_domain('icds-location-test')
        domain_name = cls.domain.name
        cls.domain_name = domain_name
        location_types = [
            'state',
            'district',
            'block',
            'supervisor',
            'awc'
        ]

        previous_parent = None
        for location_type in location_types:
            previous_parent = LocationType.objects.create(
                domain=domain_name,
                name=location_type,
                parent_type=previous_parent
            )

        cls.state = SQLLocation.objects.create(
            name='Test State',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='state')
        )
        cls.state2 = SQLLocation.objects.create(
            name='Test State2',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='state')
        )
        cls.district = SQLLocation.objects.create(
            name='Test District',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='district'),
            parent=cls.state,
        )
        cls.block = SQLLocation.objects.create(
            name='Test Block',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='block'),
            parent=cls.district
        )
        cls.supervisor = SQLLocation.objects.create(
            name='Test Supervisor',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='supervisor'),
            parent=cls.block
        )
        cls.awc = SQLLocation.objects.create(
            name='Test AWC',
            domain=domain_name,
            location_type=LocationType.objects.get(domain=domain_name, name='awc'),
            parent=cls.supervisor
        )

        cls.role = UserRole(domain=domain_name, name='demo', permissions=Permissions(access_all_locations=False))
        cls.role.save()
        cls.mobile_user = CommCareUser.create(domain_name, 'test_user', 'test_password', None, None)
        cls.mobile_user.set_location(cls.block)
        cls.mobile_user.set_role(domain_name, cls.role.get_qualified_id())
        cls.mobile_user.save()

        cls.national_user = CommCareUser.create(domain_name, 'test_user_national', 'test_password', None, None)
        cls.national_user.save()

    @classmethod
    def tearDownClass(cls):
        cls.domain.delete()
        cls.mobile_user.delete(None, None)
        cls.role.delete()
        super(TestMprAsrLocationFilter, cls).tearDownClass()

    def test_get_locations_for_restricted_user(self):
        locations = load_restricted_locs(self.domain.name,
                                         self.mobile_user.get_location_id(self.domain.name),
                                         self.mobile_user)
        expected_result = [{'name': 'Test State',
                            'location_type': 'state',
                            'uuid': mock.ANY,
                            'is_archived': False,
                            'can_edit': True,
                            'children': [{'name': 'Test District',
                                          'location_type': 'district',
                                          'uuid': mock.ANY,
                                          'is_archived': False,
                                          'can_edit': True,
                                          'children': [{'name': 'Test Block',
                                                        'location_type': 'block',
                                                        'uuid': mock.ANY,
                                                        'is_archived': False,
                                                        'can_edit': True}]}]}]
        self.assertCountEqual(expected_result, locations)

    def test_get_location_all_access_user_no_location_selected(self):
        locations = load_restricted_locs(self.domain.name, '', self.national_user)
        expected_result = [{'name': 'Test State',
                            'location_type': 'state',
                            'uuid': mock.ANY,
                            'is_archived': False,
                            'can_edit': True},
                           {'name': 'Test State2',
                            'location_type': 'state',
                            'uuid': mock.ANY,
                            'is_archived': False,
                            'can_edit': True}]
        self.assertCountEqual(expected_result, locations)

    def test_get_location_all_access_user_with_location_selected(self):
        locations = load_restricted_locs(self.domain.name, self.block.location_id, self.national_user)
        expected_result = [{'name': 'Test State',
                            'location_type': 'state',
                            'uuid': mock.ANY,
                            'is_archived': False,
                            'can_edit': True,
                            'children': [{'name': 'Test District',
                                          'location_type': 'district',
                                          'uuid': mock.ANY,
                                          'is_archived': False,
                                          'can_edit': True,
                                          'children': [{'name': 'Test Block',
                                                        'location_type': 'block',
                                                        'uuid': mock.ANY,
                                                        'is_archived': False,
                                                        'can_edit': True}]}]},
                           {'name': 'Test State2',
                            'location_type': 'state',
                            'uuid': mock.ANY,
                            'is_archived': False,
                            'can_edit': True}]
        self.assertCountEqual(expected_result, locations)
