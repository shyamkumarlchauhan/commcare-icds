import lxml.etree
import xml2json

from corehq.form_processor.submission_post import SubmissionPostFormProcessor

from custom.icds.data_vault import save_vault_entries
from custom.icds.form_processor.aadhaar_number_extractor import AadhaarNumberExtractor


class ICDSSubmissionPostFormProcessor(SubmissionPostFormProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vault_entries = []

    def pre_process_form(self):
        instance = super().pre_process_form()
        instance_xml = self.get_instance_xml(instance)
        if instance_xml:
            self.vault_entries, instance_xml = AadhaarNumberExtractor().run(instance_xml)
            instance = lxml.etree.tostring(instance_xml)
        return instance

    @staticmethod
    def get_instance_xml(instance):
        try:
            return xml2json.get_xml_from_string(instance)
        except xml2json.XMLSyntaxError:
            return None

    def post_process_form(self, xform, *args, **kwargs):
        if self.vault_entries:
            for entry in self.vault_entries:
                entry.form_id = xform.form_id
            save_vault_entries(self.vault_entries)
        return super().post_process_form(xform, *args, **kwargs)
