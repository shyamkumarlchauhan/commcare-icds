import re

from custom.icds.form_processor.steps import VaultPatternExtractor


class DummyVaultPatternExtractor(VaultPatternExtractor):
    def __init__(self):
        super(DummyVaultPatternExtractor, self).__init__(
            patterns={'secret_case_property': re.compile(r'^\d{10}$')}
        )
