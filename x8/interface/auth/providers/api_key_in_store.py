__all__ = ["APIKeyInStore"]

from x8.core import DataAccessor, Provider
from x8.core.exceptions import ForbiddenError
from x8.storage.config_store import ConfigStore
from x8.storage.document_store import DocumentStore
from x8.storage.secret_store import SecretStore

from .._models import AuthResult, UserCredential, UserInfo


class APIKeyInStore(Provider):
    store: DocumentStore | SecretStore | ConfigStore
    keys: list[str] | str | None
    field: str | None

    def __init__(
        self,
        store: DocumentStore | SecretStore | ConfigStore,
        keys: list[str] | str | None = None,
        field: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            store:
                Store that hsa the API keys.
            keys:
                Item keys in store.
            field:
                Field to use if API key is stored in a document field.
        """
        self.store = store
        self.keys = keys
        self.field = field
        super().__init__(**kwargs)

    def validate(
        self,
        credential: str | UserCredential,
    ) -> AuthResult:
        if not isinstance(credential, str):
            raise ForbiddenError("API key must be a string")
        keys = []
        if isinstance(self.keys, str):
            keys = [self.keys]
        elif isinstance(self.keys, list):
            keys = self.keys
        for key in keys:
            response = self.store.get(key)
            if isinstance(response.result.value, str):
                if response.result.value == credential:
                    return AuthResult(token=credential)
            elif isinstance(response.result.value, dict):
                if self.field is not None:
                    value = DataAccessor.get_field(
                        response.result.value,
                        self.field,
                    )
                    if value == credential:
                        return AuthResult(token=credential)
        raise ForbiddenError("Invalid API key")

    async def avalidate(
        self,
        credential: str | UserCredential,
    ) -> AuthResult:
        if not isinstance(credential, str):
            raise ForbiddenError("API key must be a string")
        keys = []
        if isinstance(self.keys, str):
            keys = [self.keys]
        elif isinstance(self.keys, list):
            keys = self.keys
        for key in keys:
            response = await self.store.aget(key)
            if isinstance(response.result.value, str):
                if response.result.value == credential:
                    return AuthResult(token=credential)
            elif isinstance(response.result.value, dict):
                if self.field is not None:
                    value = DataAccessor.get_field(
                        response.result.value,
                        self.field,
                    )
                    if value == credential:
                        return AuthResult(token=credential)
        raise ForbiddenError("Invalid API key")

    def get_user_info(
        self,
        credential: str | UserCredential,
    ) -> UserInfo:
        raise NotImplementedError(
            "API key provider does not support user info"
        )
