from typing import Any

from x8.core import DataModel


class UserCredential(DataModel):
    username: str
    password: str | None = None
    info: dict[str, Any] = dict()


class AuthResult(DataModel):
    token: str
    id: str | None = None
    email: str | None = None
    refresh_token: str | None = None
    info: dict[str, Any] = dict()


class UserInfo(DataModel):
    id: str
    name: str | None = None
    email: str | None = None
    email_verified: bool | None = None
    info: dict[str, Any] = dict()
