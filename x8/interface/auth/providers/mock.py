__all__ = ["Mock"]

from x8.core import Provider
from x8.core.exceptions import ForbiddenError

from .._models import AuthResult, UserCredential, UserInfo


class Mock(Provider):
    format: str

    def __init__(
        self,
        format: str = "csv",
        **kwargs,
    ):
        self.format = format
        super().__init__(**kwargs)

    def validate(
        self,
        credential: str | UserCredential,
    ) -> AuthResult:
        if not isinstance(credential, str):
            raise ForbiddenError("Token must be a string")
        user_info = self._parse_token(credential)
        return AuthResult(
            id=user_info.id,
            email=user_info.email,
            token=credential,
        )

    def get_user_info(
        self,
        credential: str | UserCredential,
    ) -> UserInfo:
        if not isinstance(credential, str):
            raise ForbiddenError("Token must be a string")
        return self._parse_token(credential)

    def _parse_token(self, token: str) -> UserInfo:
        if self.format == "csv":
            splits = token.split(",")
            id = splits[0]
            email = None
            name = None
            if len(splits) > 1:
                email = splits[1]
            if len(splits) > 2:
                name = splits[2]
        return UserInfo(id=id, email=email, name=name)
