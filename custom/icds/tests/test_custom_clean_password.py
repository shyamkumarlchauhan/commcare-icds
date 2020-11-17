from django.test import SimpleTestCase

import corehq.apps.domain.forms as forms

# custom.icds.commcare_extensions._complex_password
passwords = {
    'invalid': [
        'abcdefg',
        'ABCDEFG',
        '1234567',
        '!@$%^&',
        'abcdef*',
        'Abcdef*',
        '0bcdef*',
        'Ab0000*',
        'AB0000*',
        'abcdefgh',
        'ABCDEFGH',
        '12345678',
        '!@$%^&*',
        'abcdefg*',
        'Abcdefg*',
        '0bcdefg*',
        'åb0000g*',  # lowercase unicode character does not count
    ],
    'valid': [
        'Ab0000g*',
        'AB0000G*',
        'Åb0000g*',  # uppercase unicode character
    ]
}


class TestLegacyPassword(SimpleTestCase):
    def test_invalid_passwords(self):
        for password in passwords['invalid']:
            with self.assertRaises(forms.forms.ValidationError):
                forms.clean_password(password)

    def test_valid_passwords(self):
        for password in passwords['valid']:
            forms.clean_password(password)
