from __future__ import annotations

from typing import Any, Literal

from x8.core import DataModel

EmailChannel = Literal["email"]


class EmailAttachment(DataModel):
    filename: str
    content_type: str | None = None
    content: bytes | str | None = None
    content_base64: str | None = None
    disposition: Literal["attachment", "inline"] = "attachment"


class EmailContent(DataModel):
    subject: str | None = None
    text: str | None = None
    html: str | None = None
    attachments: list[EmailAttachment] | None = None


class EmailSendResult(DataModel):
    id: str
    status: str | None = None
    channel: EmailChannel = "email"
    to: list[str] = list()
    from_: str | None = None
    provider: str | None = None
    info: dict[str, Any] = dict()
