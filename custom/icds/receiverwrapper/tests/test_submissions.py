from datetime import datetime
import uuid

from django.test import TestCase
from mock import patch

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

from custom.icds.form_processor.tests.utils import DummyVaultPatternExtractor
from custom.icds.models import VaultEntry


@use_sql_backend
class SubmissionSQLTransactionsTest(TestCase, TestFileMixin):
    root = Path(__file__).parent
    file_path = ('data',)

    domain = 'icds-cas'

    def tearDown(self):
        FormProcessorTestUtils.delete_all_xforms(self.domain)
        FormProcessorTestUtils.delete_all_ledgers(self.domain)
        FormProcessorTestUtils.delete_all_cases(self.domain)
        super(SubmissionSQLTransactionsTest, self).tearDown()

    @patch('custom.icds.form_processor.tests.utils.WHITELISTED_XMLNS',
           ["http://commcarehq.org/test/submit"])
    @patch('corehq.form_processor.submission_post.XFORM_PRE_PROCESSORS', {
        domain: [DummyVaultPatternExtractor]
    })
    @patch('corehq.form_processor.backends.sql.processor.XFORM_TRACKED_MODELS',
           {domain: [VaultEntry]})
    def test_submit_with_vault_items(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.first()
        self.assertEqual(vault_entry.value, "0123456789")
        self.assertEqual(vault_entry.form_id, result.xform.form_id)

        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertFalse("0123456789" in saved_form_xml)
        self.assertTrue(
            f"<secret_case_property>vault:{vault_entry.key}"
            f"</secret_case_property>" in saved_form_xml)

    @patch('corehq.form_processor.submission_post.XFORM_PRE_PROCESSORS', {
        domain: [DummyVaultPatternExtractor]
    })
    @patch('corehq.form_processor.backends.sql.processor.XFORM_TRACKED_MODELS',
           {domain: [VaultEntry]})
    def test_no_whitelisted_submit_with_vault_entries(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.first()
        self.assertEqual(vault_entry.value, "0123456789")

        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertFalse("0123456789" in saved_form_xml)
        self.assertTrue(
            f"<secret_case_property>vault:{vault_entry.key}"
            f"</secret_case_property>" in saved_form_xml)

    @patch('custom.icds.form_processor.tests.utils.WHITELISTED_XMLNS',
           ["http://google.com/test/submit"])
    @patch('corehq.form_processor.submission_post.XFORM_PRE_PROCESSORS', {
        domain: [DummyVaultPatternExtractor]
    })
    @patch('corehq.form_processor.backends.sql.processor.XFORM_TRACKED_MODELS',
           {domain: [VaultEntry]})
    def test_non_whitelisted_submit_with_vault_entries(self):
        self.assertEqual(VaultEntry.objects.count(), 0)
        form_xml = self.get_xml('form_with_vault_item')
        result = submit_form_locally(form_xml, domain=self.domain)
        self.assertEqual(VaultEntry.objects.count(), 0)
        saved_form_xml = result.xform.get_xml().decode('utf-8')
        self.assertTrue("0123456789" in saved_form_xml)


@use_sql_backend
class FundamentalCaseTestsSQL(FundamentalBaseTests):
    domain = "icds-cas"

    @patch('custom.icds.form_processor.tests.utils.WHITELISTED_XMLNS',
           ["http://commcarehq.org/case"])
    @patch('corehq.form_processor.submission_post.XFORM_PRE_PROCESSORS', {
        domain: [DummyVaultPatternExtractor]
    })
    @patch('corehq.form_processor.backends.sql.processor.XFORM_TRACKED_MODELS',
           {domain: [VaultEntry]})
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
                'dynamic': '123', 'secret_case_property': '7777777777'
            },
            domain=self.domain,
        )
        with self.assertRaises(CaseNotFound):
            self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "7777777777")

    @patch('custom.icds.form_processor.tests.utils.WHITELISTED_XMLNS',
           ["http://commcarehq.org/case"])
    @patch('corehq.form_processor.submission_post.XFORM_PRE_PROCESSORS', {
        domain: [DummyVaultPatternExtractor]
    })
    @patch('corehq.form_processor.backends.sql.processor.XFORM_TRACKED_MODELS',
           {domain: [VaultEntry]})
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
                'dynamic': '123', 'secret_case_property': '0123456789'
            },
            domain=self.domain,
        )
        case = self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 1)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "0123456789")
        self.assertEqual(case.get_case_property('secret_case_property'),
                         f"vault:{vault_entry.key}")

        # update case
        _submit_case_block(
            False, case_id, user_id='user2', owner_id='owner2',
            case_name='update_secret_case', date_modified=modified_on,
            date_opened=modified_on, update={
                'secret_case_property': '9876543210'
            },
            domain=self.domain,
        )
        case = self.casedb.get_case(case_id)
        self.assertEqual(VaultEntry.objects.count(), 2)
        vault_entry = VaultEntry.objects.last()
        self.assertEqual(vault_entry.value, "9876543210")
        self.assertEqual(case.get_case_property('secret_case_property'),
                         f"vault:{vault_entry.key}")
