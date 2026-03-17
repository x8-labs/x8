from __future__ import annotations

from typing import Any

from x8.core import Component, Response, operation

from ._models import EmailAttachment, EmailContent, EmailSendResult


class EmailService(Component):
    from_email: str | None

    def __init__(
        self,
        from_email: str | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            from_email:
                Default sender email address.
        """
        self.from_email = from_email
        super().__init__(**kwargs)

    @operation()
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
        """Send an email with text/html and optional attachments.

        Args:
            to:
                Recipient email or list of recipients.
            content:
                Email content as plain text or structured object.
            from_email:
                Sender email address.
            cc:
                Optional CC recipient(s).
            bcc:
                Optional BCC recipient(s).
            reply_to:
                Optional reply-to email.
            attachments:
                Additional attachments.
            nconfig:
                Native provider arguments.

        Returns:
            Send result.
        """
        raise NotImplementedError

    @operation()
    async def asend(
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
        """Async variant of send."""
        raise NotImplementedError
