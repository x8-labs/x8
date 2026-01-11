__all__ = ["APIKey"]

from x8.core import Provider
from x8.core.exceptions import ForbiddenError

from .._models import AuthResult, UserCredential, UserInfo


class APIKey(Provider):
    api_keys: list[str] | str | None

    def __init__(
        self,
        api_keys: list[str] | str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_keys:
                API keys.
        """
        self.api_keys = api_keys
        super().__init__(**kwargs)

    def validate(
        self,
        credential: str | UserCredential,
    ) -> AuthResult:
        if not isinstance(credential, str):
            raise ForbiddenError("API key must be a string")
        if isinstance(self.api_keys, str):
            if credential == self.api_keys:
                return AuthResult(token=credential)
        elif isinstance(self.api_keys, list):
            if credential in self.api_keys:
                return AuthResult(token=credential)
        raise ForbiddenError("Invalid API key")

    def get_user_info(
        self,
        credential: str | UserCredential,
    ) -> UserInfo:
        raise NotImplementedError(
            "API key provider does not support user info"
        )
