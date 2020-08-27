import re

AADHAAR_XFORM_SUBMISSION_PATTERNS = {
    'aadhar_number': re.compile(r'^\d{12}$'),
    'aww_aadhar_number': re.compile(r'^\d{12}$'),
    'aadhar_number_previous': re.compile(r'^\d{12}$'),  # in cases the aadhaar_number was not extracted out earlier
}

AADHAAR_FORMS_XMLNSES = [
    'http://openrosa.org/formdesigner/1D568275-1D19-46DB-8C54-2C9765DF6335',  # Register Household
    'http://openrosa.org/formdesigner/BEB94AFD-E063-46CC-AA75-BECD3C0FC20C',  # Infrastructure Details
    'http://openrosa.org/formdesigner/991c712a8588b52505d50a6f2262ca962a85e21c',  # Edit Member
    'http://openrosa.org/formdesigner/BEB94AFD-E063-46CC-AA75-BECD3C0FC20C',  # Add Member
]
