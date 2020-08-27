from datetime import datetime
import uuid

from django.test import TestCase
from mock import patch

from corehq.apps.hqcase.utils import SYSTEM_FORM_XMLNS
from corehq.form_processor.tests.utils import (
    use_sql_backend,
    FormProcessorTestUtils,
)
from corehq.util.test_utils import TestFileMixin
from pathlib import Path

from corehq.form_processor.exceptions import CaseNotFound
from corehq.form_processor.tests.test_basics import (
    _submit_case_block,
    FundamentalBaseTests,
)
from corehq.apps.receiverwrapper.util import submit_form_locally

from custom.icds.models import VaultEntry


@use_sql_backend
class SubmissionSQLTransactionsTest(TestCase, TestFileMixin):
    root = Path(__file__).parent
    file_path = ('data',)

    domain = 'icds-cas'

    def tearDown(self):
        FormProcessorTestUtils.delete_all_xforms(self.domain)
        FormProcessorTestUtils.delete_all_cases(self.domain)
        super(SubmissionSQLTransactionsTest, self).tearDown()

    @patch('custom.icds.form_processor.aadhaar_number_extractor.AADHAAR_FORMS_XMLNSES',
           ['http://commcarehq.org/test/submit'])
    def test_submit_with_vault_items(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.first()
        self.assertEqual(vault_entry.value, "123456789012")

        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertFalse("123456789012" in saved_form_xml)
        self.assertTrue(
            f"<aadhar_number>vault:{vault_entry.key}"
            f"</aadhar_number>" in saved_form_xml)

    @patch('custom.icds.form_processor.aadhaar_number_extractor.AADHAAR_FORMS_XMLNSES', [])
    def test_no_whitelisted_submit_with_vault_entries(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 0)
        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertTrue("123456789012" in saved_form_xml)

    @patch('custom.icds.form_processor.aadhaar_number_extractor.AADHAAR_FORMS_XMLNSES',
           ["http://google.com/test/submit"])
    def test_non_whitelisted_submit_with_vault_entries(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 0)
        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertTrue("123456789012" in saved_form_xml)


@use_sql_backend
@patch('custom.icds.form_processor.aadhaar_number_extractor.AADHAAR_FORMS_XMLNSES',
       [SYSTEM_FORM_XMLNS])
class FundamentalCaseTestsSQL(FundamentalBaseTests):
    domain = "icds-cas"

    def test_failed_form_with_case_with_secret(self):
        case_id = uuid.uuid4().hex
        modified_on = datetime.utcnow()
        self.assertEqual(VaultEntry.objects.count(), 0)

        _submit_case_block(
            True, case_id, user_id='user1', owner_id='owner1',
            case_type='demo',
            case_name='this is a very long case name that exceeds the '
                      '255 char limit' * 5,
            date_modified=modified_on, date_opened=modified_on, update={
                'dynamic': '123', 'aadhar_number': '123456789012'
            },
            domain=self.domain,
        )
        with self.assertRaises(CaseNotFound):
            self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "123456789012")

    def test_successful_form_with_case_with_secret(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        case_id = uuid.uuid4().hex
        modified_on = datetime.utcnow()
        # create case
        _submit_case_block(
            True, case_id, user_id='user1', owner_id='owner1',
            case_type='demo',
            case_name='secret_case', date_modified=modified_on,
            date_opened=modified_on, update={
                'dynamic': '123', 'aadhar_number': '123456789012'
            },
            domain=self.domain,
        )
        case = self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "123456789012")
        self.assertEqual(case.get_case_property('aadhar_number'),
                         f"vault:{vault_entry.key}")

        # update case
        _submit_case_block(
            False, case_id, user_id='user2', owner_id='owner2',
            case_name='update_secret_case', date_modified=modified_on,
            date_opened=modified_on, update={
                'aadhar_number': '123456789013'
            },
            domain=self.domain,
        )
        case = self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 2)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "123456789013")
        self.assertEqual(case.get_case_property('aadhar_number'),
                         f"vault:{vault_entry.key}")
