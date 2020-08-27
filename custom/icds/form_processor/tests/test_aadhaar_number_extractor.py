from unittest import TestCase

from custom.icds.form_processor.consts import AADHAAR_XFORM_SUBMISSION_PATTERNS


class TestAadhaarPattern(TestCase):
    def test_aadhaar_number_pattern(self):
        aadhaar_number_pattern = AADHAAR_XFORM_SUBMISSION_PATTERNS['aadhar_number']
        self.assertTrue(aadhaar_number_pattern.match('123456789012'))
        self.assertFalse(aadhaar_number_pattern.match('12345678901'))
