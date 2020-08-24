import xml2json

from abc import ABC, abstractmethod

from custom.icds.data_vault import new_vault_entry
from custom.icds.form_processor.consts import (
    AADHAAR_XFORM_SUBMISSION_PATTERNS,
    AADHAAR_FORMS_XMLNSES,
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
    """
    Extract based on tag of xml nodes containing text that matches a regex.
    Pattern example: {'secret_case_property': re.compile(r'^\d{10}$')}
    to match any node 'secret_case_property' with text that is 10 digits
    """
    def __init__(self, patterns, xmlns_whitelist=None):
        self._patterns = patterns
        self._xmlns_whitelist = xmlns_whitelist or []

    def __call__(self, context):
        if self._should_process(context.instance_xml):
            vault_entries = self._replace_matches(context.instance_xml)
            context.supplementary_models.extend(vault_entries)

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


class AadhaarNumberExtractor(VaultPatternExtractor):
    identifier = "AadhaarNumber"

    def __init__(self):
        super(AadhaarNumberExtractor, self).__init__(
            patterns=AADHAAR_XFORM_SUBMISSION_PATTERNS,
            xmlns_whitelist=AADHAAR_FORMS_XMLNSES
        )
