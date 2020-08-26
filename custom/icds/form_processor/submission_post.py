from corehq.form_processor.submission_post import SubmissionPostFormProcessor

from custom.icds.data_vault import save_vault_entries
from custom.icds.form_processor.aadhaar_number_extractor import AadhaarNumberExtractor


class ICDSSubmissionPostFormProcessor(SubmissionPostFormProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vault_entries = []

    def pre_process_form(self, *args, **kwargs):
        self.vault_entries = AadhaarNumberExtractor().run(self)
        return super().pre_process_form(*args, **kwargs)

    def post_process_form(self, xform, *args, **kwargs):
        if self.vault_entries:
            for entry in self.vault_entries:
                entry.form_id = xform.form_id
            save_vault_entries(self.vault_entries)
        return super().post_process_form(xform, *args, **kwargs)
