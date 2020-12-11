import json
import os
import shutil
import tarfile
import uuid
import zipfile
from datetime import datetime

from django.core.management import call_command

from casexml.apps.case.mock import CaseStructure, CaseIndex, CaseFactory
from corehq.apps.domain.models import Domain
from corehq.apps.dump_reload.management.commands.load_domain_data import get_tmp_extract_dir
from corehq.apps.dump_reload.tests.test_couch_dump_load import delete_doain_couch_data_for_dump_load_test
from corehq.apps.dump_reload.tests.test_sql_dump_load import BaseDumpLoadTest, delete_domain_sql_data_for_dump_load_test
from corehq.apps.locations.tests.util import setup_locations_and_types
from corehq.apps.mobile_auth.models import MobileAuthKeyRecord
from corehq.apps.products.models import SQLProduct
from corehq.apps.sms.models import PhoneNumber
from corehq.apps.users.models import CommCareUser
from corehq.elastic import get_es_new
from corehq.form_processor.tests.utils import use_sql_backend
from corehq.pillows.mappings.user_mapping import USER_INDEX_INFO
from corehq.toggles import all_toggles, NAMESPACE_DOMAIN
from corehq.util.es.testing import sync_users_to_es
from corehq.util.test_utils import trap_extra_setup
from pillowtop.es_utils import initialize_index_and_mapping
from toggle.shortcuts import set_toggle


@use_sql_backend
class TestDumpLoadByLocation(BaseDumpLoadTest):
    """
    Test the round trip of dumping data for a single state and loading it back.

    The goal of this test is to ensure that the files produced by the dump command
    can be read successfully by the load command.

    This does not test the integrity of the loaded data since that is tested elsewhere but
    it does test that the loaded counts match what is expected for data at the specified
    location.
    """
    def test(self):
        self._prepare_filter_files()
        self._dump_domain_data()
        self._delete_domain_data()
        self._load_domain_data()

    def setUp(self):
        with trap_extra_setup(ConnectionError):
            es = get_es_new()
            initialize_index_and_mapping(es, USER_INDEX_INFO)

        self.domain_name = uuid.uuid4().hex
        self.domain = Domain(name=self.domain_name)
        self.domain.save()

        self.state = 'state1'
        self.dump_file_path = f"state_dump_{self.state}_{uuid.uuid4().hex}.zip"

        set_toggle(all_toggles()[0].slug, "user1", True)
        set_toggle(all_toggles()[1].slug, self.domain_name, True, namespace=NAMESPACE_DOMAIN)

        SQLProduct.objects.create(domain=self.domain_name, product_id='test1', name='test1')

        loc_types, locations = setup_locations_and_types(
            self.domain_name,
            ['state', 'district', 'city'],
            [],
            [
                (self.state, [
                    ('district1 ', [
                        ('user1', []),
                        ('user2', []),
                    ]),
                ]),
                ('state2', [
                    ('district2 ', [
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
            with sync_users_to_es():
                user.set_location(location)
            MobileAuthKeyRecord(
                domain=self.domain_name, user_id=username, valid=datetime.utcnow(), expires=datetime.utcnow()
            ).save()
            PhoneNumber.objects.create(
                domain=self.domain_name,
                owner_doc_type='CommCareUser',
                owner_id=user.user_id,
                pending_verification=False,
                is_two_way=False
            )

            cases = factory.create_or_update_case(
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
            PhoneNumber.objects.create(
                domain=self.domain_name,
                owner_doc_type='CommCareCase',
                owner_id=cases[0].case_id,
                pending_verification=False,
                is_two_way=False
            )

        self.expected_meta = {
            "domain": {"Domain": 1},
            "sql-sql-sharded-blob_meta-default": {"blobs.BlobMeta": 2},
            "sql-sharded-default": {
                "form_processor.XFormInstanceSQL": 2,
                "form_processor.CommCareCaseSQL": 6,
                "form_processor.CommCareCaseIndexSQL": 2,
                "form_processor.CaseTransaction": 6,
                'sms.PhoneNumber': 2
            },
            "sql": {
                "locations.LocationType": 3,
                "products.SQLProduct": 1,
                "auth.User": 2,
                "locations.SQLLocation": 4,
                'sms.PhoneNumber': 2
            },
            "couch": {"users.CommCareUser": 2},
            "toggles": {"Toggle": 2}
        }

    def tearDown(self):
        self._delete_domain_data()
        _cleanup_files([self.dump_file_path])

    def _load_domain_data(self):
        extract_dir = get_tmp_extract_dir(self.dump_file_path)
        with zipfile.ZipFile(self.dump_file_path, 'r') as archive:
            archive.extractall(extract_dir)
        self.addCleanup(_cleanup_files, [extract_dir])
        output = call_command("load_domain_data", self.dump_file_path, use_extracted=True, json_output=True)
        self.assertDictEqual(json.loads(output), self.expected_meta)

    def _delete_domain_data(self):
        delete_domain_sql_data_for_dump_load_test(self.domain_name)
        delete_doain_couch_data_for_dump_load_test(self.domain_name)
        domain = Domain.get_by_name(self.domain_name)
        if domain:
            domain.delete()

    def _dump_domain_data(self):
        call_command("dump_data_by_location", self.domain_name, self.state, output_path=self.dump_file_path, thread_count=0)
        blob_meta_name = 'sql-sql-sharded-blob_meta-default.gz'
        with zipfile.ZipFile(self.dump_file_path, 'r') as archive:
            archive_files = archive.namelist()
            archive.extract(blob_meta_name)
            self.addCleanup(_cleanup_files, [blob_meta_name])
            meta = json.loads(archive.read("meta.json"))
        self.assertEqual(set(archive_files), {
            'domain.gz', 'toggles.gz', 'couch.gz', 'sql.gz',
            'sql-sharded-default.gz', blob_meta_name, 'meta.json'
        })
        self.assertEqual(meta, self.expected_meta)

        self._dump_blobs(blob_meta_name)

    def _dump_blobs(self, blob_meta_filename):
        output = call_command("export_selected_blobs", blob_meta_filename, json_output=True)
        blob_path = json.loads(output)["path"]
        self.addCleanup(_cleanup_files, [blob_path])
        with tarfile.open(blob_path, 'r:gz') as tgzfile:
            self.assertEqual(len(tgzfile.getnames()), 2)

    def _prepare_filter_files(self):
        paths = call_command("prepare_filter_values_for_location_dump", self.domain_name, self.state, json_output=True)
        self.addCleanup(_cleanup_files, json.loads(paths)["paths"])


def _cleanup_files(paths):
    def _remove_path(path_to_remove):
        if not os.path.exists(path_to_remove):
            return
        if os.path.isdir(path_to_remove):
            shutil.rmtree(path_to_remove)
        else:
            os.remove(path_to_remove)

    if isinstance(paths, dict):
        for name, path in paths.items():
            _remove_path(path)
    elif isinstance(paths, list):
        for path in paths:
            _remove_path(path)
