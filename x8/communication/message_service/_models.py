from __future__ import annotations

from typing import Any, Literal

from x8.core import DataModel

MessageChannel = Literal[
    "sms",
    "whatsapp",
    "telegram",
    "messenger",
    "rcs",
    "other",
]


class MessageMedia(DataModel):
    url: str
    content_type: str | None = None
    caption: str | None = None


class MessageContent(DataModel):
    text: str | None = None
    media: list[MessageMedia] | None = None


class MessageSendResult(DataModel):
    id: str
    status: str | None = None
    channel: MessageChannel | None = None
    to: str | None = None
    from_: str | None = None
    provider: str | None = None
    price: str | None = None
    currency: str | None = None
    info: dict[str, Any] = dict()


class VerificationStartResult(DataModel):
    id: str
    status: str | None = None
    channel: MessageChannel | None = None
    to: str | None = None
    provider: str | None = None
    info: dict[str, Any] = dict()


class VerificationCheckResult(DataModel):
    id: str
    status: str | None = None
    approved: bool | None = None
    valid: bool | None = None
    to: str | None = None
    provider: str | None = None
    info: dict[str, Any] = dict()
