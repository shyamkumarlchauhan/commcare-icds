import re

AADHAAR_XFORM_SUBMISSION_PATTERNS = {
    'aadhar_number': re.compile(r'^\d{12}$')
}
# ToDo: fill the xmlnses
AADHAAR_FORMS_XMLNSES = []

