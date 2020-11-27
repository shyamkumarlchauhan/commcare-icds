import uuid
import zipfile
from datetime import datetime

from django.core.management import call_command

from casexml.apps.case.mock import CaseStructure, CaseIndex, CaseFactory
from corehq.apps.domain.models import Domain
from corehq.apps.dump_reload.tests.test_couch_dump_load import delete_doain_couch_data_for_dump_load_test
from corehq.apps.dump_reload.tests.test_sql_dump_load import BaseDumpLoadTest, delete_domain_sql_data_for_dump_load_test
from corehq.apps.locations.tests.util import setup_locations_and_types
from corehq.apps.mobile_auth.models import MobileAuthKeyRecord
from corehq.apps.products.models import SQLProduct
from corehq.apps.users.models import CommCareUser
from corehq.form_processor.tests.utils import use_sql_backend
from corehq.toggles import all_toggles
from toggle.shortcuts import set_toggle


@use_sql_backend
class TestSimpleSQLExport(BaseDumpLoadTest):
    def setUp(self):
        self.domain_name = uuid.uuid4().hex
        self.domain = Domain(name=self.domain_name)
        self.domain.save()

        set_toggle(all_toggles()[0].slug, "user1", True)

        SQLProduct.objects.create(domain=self.domain_name, product_id='test1', name='test1')
        loc_types, locations = setup_locations_and_types(
            self.domain_name,
            ['state', 'district', 'city'],
            [],
            [
                ('Delhi', [
                    ('Ekurhuleni ', [
                        ('user1', []),
                        ('user2', []),
                        ('user3', []),
                    ]),
                ]),
            ],
        )
        for loc_type in loc_types.values():
            loc_type.shares_cases = True
            loc_type.save()

        factory = CaseFactory(domain=self.domain_name)
        for username in ("user1", "user2", "user3"):
            user = CommCareUser.create(
                self.domain_name, username=username, password="123", created_by="", created_via="",
            )
            location = locations[username]
            user.set_location(location)
            MobileAuthKeyRecord(
                domain=self.domain_name, user_id=username, valid=datetime.utcnow(), expires=datetime.utcnow()
            ).save()

            factory.create_or_update_case(
                CaseStructure(
                    attrs={'case_name': f'{username} child', 'owner_id': location.location_id, 'create': True},
                    indices=[
                        CaseIndex(CaseStructure(attrs={
                            'case_name': f'{username} parent',
                            'owner_id': location.location_id,
                            'create': True
                        })),
                    ]
                ), user_id=user.user_id
            )

    def tearDown(self):
        delete_domain_sql_data_for_dump_load_test(self.domain_name)
        delete_doain_couch_data_for_dump_load_test(self.domain_name)
        domain = Domain.get_by_name(self.domain_name)
        if domain:
            domain.delete()

    def test_dump_and_load(self):
        state = 'Delhi'
        call_command("prepare_filter_values_for_state_dump", self.domain_name, state)
        file_path = f"state_dump_{state}_{uuid.uuid4().hex}.zip"
        call_command("dump_data_by_location", self.domain_name, state, output_path=file_path, dumper="sql-sharded")
        with zipfile.ZipFile(file_path, 'r') as archive:
            print(archive.namelist())

        delete_domain_sql_data_for_dump_load_test(self.domain_name)
        delete_doain_couch_data_for_dump_load_test(self.domain_name)
        self.domain.delete()

        output = call_command("load_domain_data", file_path)
        self.assertDictEqual(output, {})
        # assert state

