import xml2json

from custom.icds.data_vault import (
    new_vault_entry,
)
from custom.icds.form_processor.consts import (
    AADHAAR_XFORM_SUBMISSION_PATTERNS,
    AADHAAR_FORMS_XMLNSES,
)


class AadhaarNumberExtractor(object):
    """
     Extract based on tag of xml nodes containing text that matches a regex.
     Pattern example: {'aadhar_number': re.compile(r'^\d{12}$')}
     to match any node 'aadhar_number' with text that is 12 digits
     """
    def __init__(self):
        self._patterns = AADHAAR_XFORM_SUBMISSION_PATTERNS
        self._xmlns_whitelist = AADHAAR_FORMS_XMLNSES

    def run(self, form_processor):
        vault_entries = None
        instance_xml = form_processor.get_instance_xml()
        if instance_xml and self._should_process(instance_xml):
            vault_entries = self._replace_matches(instance_xml)
            if vault_entries:
                form_processor.update_instance(instance_xml)
        return vault_entries

    def _should_process(self, xml):
        if not self._xmlns_whitelist:
            return True
        tag, xmlns = xml2json.get_tag_and_xmlns(xml)
        return not xmlns or xmlns in self._xmlns_whitelist

    def _replace_matches(self, xml):
        vault_entries = {}
        for element in xml.iter():
            tag, xmlns = xml2json.get_tag_and_xmlns(element)
            text = element.text
            if self._should_replace(tag, text):
                if element.text not in vault_entries:
                    vault_entries[text] = new_vault_entry(text)
                element.text = f"vault:{vault_entries[text].key}"
        return vault_entries.values()

    def _should_replace(self, tag, text):
        for tag_name, tag_value_regex in self._patterns.items():
            if tag_name == tag and tag_value_regex.match(text):
                return True
        return False

