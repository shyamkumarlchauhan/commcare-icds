import re

from custom.icds.form_processor.steps import VaultPatternExtractor


# convenient way to patch in tests
WHITELISTED_XMLNS = []


class DummyVaultPatternExtractor(VaultPatternExtractor):
    def __init__(self):
        super(DummyVaultPatternExtractor, self).__init__(
            patterns={'secret_case_property': re.compile(r'^\d{10}$')},
            xmlns_whitelist=WHITELISTED_XMLNS
        )
