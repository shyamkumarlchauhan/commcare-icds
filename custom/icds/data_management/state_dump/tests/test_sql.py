import json
import uuid
from datetime import datetime
from io import StringIO

from django.contrib.auth.models import User
from mock import Mock

from casexml.apps.case.mock import CaseStructure, CaseIndex, CaseFactory
from corehq.apps.cloudcare.models import ApplicationAccess
from corehq.apps.commtrack.tests.util import get_single_balance_block
from corehq.apps.commtrack.helpers import make_product
from corehq.apps.dump_reload.tests.test_sql_dump_load import BaseDumpLoadTest
from corehq.apps.hqcase.utils import submit_case_blocks
from corehq.apps.locations.models import LocationType, SQLLocation
from corehq.apps.locations.tests.util import setup_locations_and_types
from corehq.apps.mobile_auth.models import SQLMobileAuthKeyRecord
from corehq.apps.products.models import SQLProduct
from corehq.apps.users.models import CommCareUser
from corehq.form_processor.models import XFormInstanceSQL, XFormOperationSQL, CommCareCaseSQL, CaseTransaction, \
    LedgerValue, LedgerTransaction, CommCareCaseIndexSQL
from corehq.form_processor.tests.utils import create_form_for_test, use_sql_backend
from custom.icds.data_management.state_dump.sql import dump_simple_sql_data, dump_form_case_data


class TestSimpleSQLExport(BaseDumpLoadTest):
    other_domain = uuid.uuid4().hex

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.default_objects_counts = {}

    def _get_dump_fn(self, context):
        def dump(output_stream):
            return dump_simple_sql_data(self.domain_name, context, output_stream, None)
        return dump

    def test(self):
        SQLProduct.objects.create(domain=self.domain_name, product_id='test1', name='test1')

        # should be excluded since it's in another domain
        p2 = SQLProduct.objects.create(domain=self.other_domain, product_id='test2', name='test2')
        self.addCleanup(p2.delete)

        # should be excluded based on the exclude list
        ApplicationAccess.objects.create(domain=self.domain_name)

        setup_locations_and_types(
            self.domain_name,
            ['province', 'district', 'city'],
            [],
            [
                ('Western Cape', [
                    ('Cape Winelands', [
                        ('Stellenbosch', []),
                        ('Paarl', []),
                    ]),
                    ('Cape Town', [
                        ('Cape Town City', []),
                    ])
                ]),
                ('Gauteng', [
                    ('Ekurhuleni ', [
                        ('Alberton', []),
                        ('Benoni', []),
                    ]),
                ]),
            ],
        )

        for user_id in ("user1", "user2", "user3"):
            CommCareUser.create(
                self.domain_name, username=user_id, password="123", created_by="", created_via="",
                uuid=user_id
            )
            SQLMobileAuthKeyRecord(
                domain=self.domain_name, user_id=user_id, valid=datetime.utcnow(), expires=datetime.utcnow()
            ).save(sync_to_couch=False)

        # should be excluded based on exclude list
        create_form_for_test(self.domain_name)


        location_ids = {
            location.location_id
            for location in SQLLocation.objects.get(name="Western Cape").get_descendants(include_self=True)
        }
        context = _get_mock_context(location_ids, {"user1", "user2"}, [])
        expected = {
            SQLLocation: 6,
            LocationType: 3,
            SQLProduct: 1,
            SQLMobileAuthKeyRecord: 2,
            User: 2
        }
        self._dump_and_load(expected, dumper_fn=self._get_dump_fn(context))
        self.assertEqual(context.user_ids, {u.username for u in User.objects.all()})
        self.assertEqual(context.user_ids, {u.user_id for u in SQLMobileAuthKeyRecord.objects.all()})
        self.assertEqual(context.location_ids, {loc.location_id for loc in SQLLocation.objects.all()})


@use_sql_backend
class TestFormCaseSQLExport(BaseDumpLoadTest):
    other_domain = uuid.uuid4().hex

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.default_objects_counts = {}
        cls.product = make_product(cls.domain_name, 'A Product', 'prodcode_a')

    def _get_dump_fn(self, context, blob_output):
        def dump(output_stream):
            return dump_form_case_data(self.domain_name, context, output_stream, blob_output)
        return dump

    def test_forms(self):
        filtered_users = {"user1", "user2"}
        expected_form_ids = set()
        for user_id in ("user1", "user2", "user3"):
            form = create_form_for_test(self.domain_name, user_id=user_id, case_id=uuid.uuid4().hex)
            XFormOperationSQL.objects.using(form.db).create(
                form=form, user_id="anything", operation="something", date=datetime.utcnow()
            )
            if user_id in filtered_users:
                expected_form_ids.add(form.form_id)

        context = _get_mock_context([], filtered_users, [])
        # Cases not included because owner_ids in context is empty
        expected = {
            XFormInstanceSQL: 2,
            XFormOperationSQL: 2
        }
        blob_output = StringIO()
        self._dump_and_load(expected, dumper_fn=self._get_dump_fn(context, blob_output))
        actual_blob_counts, blob_dump_lines = self._parse_dump_output(blob_output)
        self.assertEqual(len(blob_dump_lines), 2)
        form_ids = {json.loads(line)['fields']['parent_id'] for line in blob_dump_lines}
        self.assertEqual(form_ids, expected_form_ids)

    def test_cases(self):
        factory = CaseFactory(domain=self.domain_name)

        filtered_owners = {"user1", "user2"}
        expected_case_ids = set()
        for owner_id in ("user1", "user2", "user3"):
            cases = factory.create_or_update_case(
                CaseStructure(
                    attrs={'case_name': f'{owner_id} child', 'owner_id': owner_id, 'create': True},
                    indices=[
                        CaseIndex(CaseStructure(attrs={
                            'case_name': f'{owner_id} parent',
                            'owner_id': owner_id,
                            'create': True
                        })),
                    ]
                ), user_id=owner_id
            )
            submit_case_blocks([
                get_single_balance_block(cases[0].case_id, self.product._id, 10)
            ], self.domain_name)

            if owner_id in filtered_owners:
                expected_case_ids.union({c.case_id for c in cases})

        context = _get_mock_context([], [], filtered_owners)
        # Forms not included because user_ids in context is empty
        expected = {
            CommCareCaseSQL: 4,
            CommCareCaseIndexSQL: 2,
            CaseTransaction: 6,
            LedgerValue: 2,
            LedgerTransaction: 2
        }

        blob_output = StringIO()
        self._dump_and_load(expected, dumper_fn=self._get_dump_fn(context, blob_output))
        actual_blob_counts, blob_dump_lines = self._parse_dump_output(blob_output)
        self.assertEqual(len(blob_dump_lines), 0)


def _get_mock_context(location_ids, user_ids, owner_ids):
    context = Mock()
    context.location_ids = location_ids
    context.user_ids = user_ids
    context.owner_ids = owner_ids
    context.types = None
    return context
