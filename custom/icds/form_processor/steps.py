import xml2json

from abc import ABC, abstractmethod

from custom.icds.data_vault import new_vault_entry, save_vault_entries
from custom.icds.form_processor.consts import (
    AADHAAR_XFORM_SUBMISSION_PATTERNS,
)


class FormProcessingStep(ABC):
    @abstractmethod
    def __call__(self, context):
        """
        Process the form context object. Return an instance of
        FormProcessingResult to terminate the processing or None
        To allow processing to continue to the next step
        :param context: form context
        """
        pass


class VaultPatternExtractor(FormProcessingStep):
    def __init__(self, patterns):
        self._patterns = patterns

    def __call__(self, context):
        return self._process(context)

    def _process(self, context):
        new_xml = self._replace_values(context.instance_xml)
        context.instance_xml = new_xml

    def _replace_values(self, xml):
        vault_entries = self._replace_matches(xml)
        if vault_entries:
            save_vault_entries(vault_entries)
        return xml

    def _replace_matches(self, xml):
        vault_entries = {}
        for element in xml.iter():
            tag, xmlns = xml2json.get_tag_and_xmlns(element)
            text = element.text
            if self._matches(tag, text):
                if element.text not in vault_entries:
                    vault_entries[text] = new_vault_entry(text)
                element.text = f"vault:{vault_entries[text].key}"
        return vault_entries.values()

    def _matches(self, tag, text):
        for tag_name, tag_value_regex in self._patterns.items():
            if tag_name == tag and tag_value_regex.match(text):
                return True
        return False


class AadhaarNumberExtractor(VaultPatternExtractor):
    identifier = "AadhaarNumber"

    def __init__(self):
        super(AadhaarNumberExtractor, self).__init__(
            patterns=AADHAAR_XFORM_SUBMISSION_PATTERNS,
        )
