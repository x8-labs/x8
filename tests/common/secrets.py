from x8.storage.secret_store import SecretStore
from x8.storage.secret_store.providers.google_secret_manager import (
    GoogleSecretManager,
)


def get_secrets():
    secret_store = SecretStore(__provider__=GoogleSecretManager())
    return secret_store
