from custom.icds.data_management.base import SQLBasedDataManagement
from custom.icds.data_management.doc_processors import (
    PopulateMissingMotherNameDocProcessor,
    SanitizePhoneNumberDocProcessor,
)


class PopulateMissingMotherName(SQLBasedDataManagement):
    slug = "populate_missing_mother_name"
    name = "Populate missing mother name"
    case_type = "person"
    doc_processor = PopulateMissingMotherNameDocProcessor


class SanitizePhoneNumber(SQLBasedDataManagement):
    slug = "sanitize_phone_number"
    name = "Sanitize phone number"
    case_type = "person"
    doc_processor = SanitizePhoneNumberDocProcessor
