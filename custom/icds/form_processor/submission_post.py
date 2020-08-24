from corehq.form_processor.submission_context import SubmissionFormContext

from custom.icds.form_processor.steps import AadhaarNumberExtractor


class ICDSSubmissionFormContext(SubmissionFormContext):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.supplementary_models = []
        AadhaarNumberExtractor()(self)

    def post_process_form(self, xform):
        [xform.track_create(model_obj) for model_obj in self.supplementary_models]
        super().post_process_form(xform)
