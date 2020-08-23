def new_vault_entry(value):
    from custom.icds.models import VaultEntry
    return VaultEntry(value=value)
