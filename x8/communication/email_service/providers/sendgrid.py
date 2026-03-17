from __future__ import annotations

import base64
import uuid
from typing import Any, NoReturn

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Attachment,
    Bcc,
    Cc,
    Content,
    Disposition,
    FileContent,
    FileName,
    FileType,
    Mail,
    To,
)

from x8.core import Provider, Response
from x8.core.exceptions import BadRequestError, UnauthorizedError

from .._models import EmailAttachment, EmailContent, EmailSendResult

__all__ = ["SendGrid"]


class SendGrid(Provider):
    api_key: str | None
    from_email: str | None
    nparams: dict[str, Any]

    _client: SendGridAPIClient | None

    def __init__(
        self,
        api_key: str | None = None,
        from_email: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            api_key:
                SendGrid API key.
            from_email:
                Default sender email.
            nparams:
                Native parameters for SendGrid API client.
        """
        self.api_key = api_key
        self.from_email = from_email
        self.nparams = nparams or {}
        self._client = None
        super().__init__(**kwargs)

    def __setup__(self, context=None) -> None:
        if self._client is not None:
            return
        self._client = SendGridAPIClient(
            api_key=self.api_key,
            **self.nparams,
        )

    def send(
        self,
        to: str | list[str],
        content: str | EmailContent,
        *,
        from_email: str | None = None,
        cc: str | list[str] | None = None,
        bcc: str | list[str] | None = None,
        reply_to: str | None = None,
        attachments: list[dict | EmailAttachment] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[EmailSendResult]:
        self.__setup__()
        recipients = self._normalize_list(to)
        if not recipients:
            raise BadRequestError("At least one recipient is required")

        payload = self._as_content(content)
        merged_attachments = list(payload.attachments or [])
        for item in attachments or []:
            merged_attachments.append(
                EmailAttachment.from_dict(item)
                if isinstance(item, dict)
                else item
            )

        subject = payload.subject or "Notification"
        sender = (
            from_email
            or self.from_email
            or getattr(self.__component__, "from_email", None)
        )
        if sender is None:
            raise BadRequestError("Sender email is required")

        mail = Mail(
            from_email=sender,
            to_emails=[To(email) for email in recipients],
            subject=subject,
        )
        if payload.text is not None:
            mail.add_content(Content("text/plain", payload.text))
        if payload.html is not None:
            mail.add_content(Content("text/html", payload.html))
        if payload.text is None and payload.html is None:
            raise BadRequestError("Email content requires text or html")

        if reply_to is not None:
            mail.reply_to = To(reply_to)
        for email in self._normalize_list(cc):
            mail.add_cc(Cc(email))
        for email in self._normalize_list(bcc):
            mail.add_bcc(Bcc(email))
        for attachment in merged_attachments:
            mail.add_attachment(self._convert_attachment(attachment))

        args = nconfig or {}
        try:
            assert self._client is not None
            result = self._client.send(mail, **args)
        except Exception as error:
            self._raise_provider_error(error)

        message_id = result.headers.get("X-Message-Id", str(uuid.uuid4()))
        return Response(
            result=EmailSendResult(
                id=message_id,
                status=str(result.status_code),
                to=recipients,
                from_=sender,
                provider="sendgrid",
                info={
                    "status_code": result.status_code,
                    "headers": dict(result.headers),
                    "body": result.body,
                },
            )
        )

    def _as_content(self, content: str | EmailContent) -> EmailContent:
        if isinstance(content, str):
            return EmailContent(text=content)
        return content

    def _normalize_list(
        self,
        value: str | list[str] | None,
    ) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    def _convert_attachment(self, item: EmailAttachment) -> Attachment:
        encoded = item.content_base64
        if encoded is None:
            if item.content is None:
                raise BadRequestError(
                    f"Attachment {item.filename} missing content"
                )
            if isinstance(item.content, str):
                raw = item.content.encode("utf-8")
            else:
                raw = item.content
            encoded = base64.b64encode(raw).decode("utf-8")
        attachment = Attachment()
        attachment.file_content = FileContent(encoded)
        attachment.file_name = FileName(item.filename)
        attachment.disposition = Disposition(item.disposition)
        if item.content_type is not None:
            attachment.file_type = FileType(item.content_type)
        return attachment

    def _raise_provider_error(self, error: Exception) -> NoReturn:
        message = str(error) if str(error) else "SendGrid API error"
        if "401" in message or "unauthorized" in message.lower():
            raise UnauthorizedError(message) from error
        raise BadRequestError(message) from error
