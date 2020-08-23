def new_vault_entry(value):
    from custom.icds.models import VaultEntry
    return VaultEntry(value=value)


def save_vault_entries(entries):
    from custom.icds.models import VaultEntry
    return VaultEntry.objects.bulk_create(entries)
