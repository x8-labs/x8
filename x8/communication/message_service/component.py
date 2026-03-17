from __future__ import annotations

from typing import Any

from x8.core import Component, Response, operation

from ._models import (
    MessageChannel,
    MessageContent,
    MessageMedia,
    MessageSendResult,
    VerificationCheckResult,
    VerificationStartResult,
)


class MessageService(Component):
    from_: str | None
    messaging_service_sid: str | None

    def __init__(
        self,
        from_: str | None = None,
        messaging_service_sid: str | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            from_:
                Default sender address. Examples: "+15558675310",
                "whatsapp:+14155238886".
            messaging_service_sid:
                Optional Twilio messaging service SID used as default.
        """
        self.from_ = from_
        self.messaging_service_sid = messaging_service_sid
        super().__init__(**kwargs)

    @operation()
    def send(
        self,
        to: str,
        content: str | MessageContent,
        *,
        channel: MessageChannel | None = None,
        from_: str | None = None,
        media: list[dict | MessageMedia] | None = None,
        messaging_service_sid: str | None = None,
        status_callback: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[MessageSendResult]:
        """Send text/media message over SMS, WhatsApp, or other channels.

        Args:
            to:
                Receiver address. Examples: "+15558675310",
                "whatsapp:+15558675310".
            content:
                Message content as plain text or structured object.
            channel:
                Message channel. If omitted, provider infers from address.
            from_:
                Sender address. If omitted, component/provider defaults apply.
            media:
                Optional media attachments.
            messaging_service_sid:
                Optional provider-specific messaging service reference.
            status_callback:
                Optional callback URL for delivery updates.
            nconfig:
                Native provider arguments.

        Returns:
            Send result.
        """
        raise NotImplementedError

    @operation()
    def initiate_verification(
        self,
        to: str,
        *,
        channel: MessageChannel | None = "sms",
        locale: str | None = None,
        custom_friendly_name: str | None = None,
        code_length: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VerificationStartResult]:
        """Initiate a verification flow.

        Args:
            to:
                Receiver address.
            channel:
                Verification channel. Example: "sms" or "whatsapp".
            locale:
                Locale used by the verification template when supported.
            custom_friendly_name:
                Optional verification friendly name.
            code_length:
                Optional code length.
            nconfig:
                Native provider arguments.

        Returns:
            Verification start result.
        """
        raise NotImplementedError

    @operation()
    def verify(
        self,
        to: str,
        code: str,
        *,
        channel: MessageChannel | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VerificationCheckResult]:
        """Verify an OTP or verification code.

        Args:
            to:
                Receiver address.
            code:
                Verification code.
            channel:
                Optional channel hint for address normalization.
            nconfig:
                Native provider arguments.

        Returns:
            Verification check result.
        """
        raise NotImplementedError

    @operation()
    async def asend(
        self,
        to: str,
        content: str | MessageContent,
        *,
        channel: MessageChannel | None = None,
        from_: str | None = None,
        media: list[dict | MessageMedia] | None = None,
        messaging_service_sid: str | None = None,
        status_callback: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[MessageSendResult]:
        """Async variant of send."""
        raise NotImplementedError

    @operation()
    async def ainitiate_verification(
        self,
        to: str,
        *,
        channel: MessageChannel | None = "sms",
        locale: str | None = None,
        custom_friendly_name: str | None = None,
        code_length: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VerificationStartResult]:
        """Async variant of initiate_verification."""
        raise NotImplementedError

    @operation()
    async def averify(
        self,
        to: str,
        code: str,
        *,
        channel: MessageChannel | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VerificationCheckResult]:
        """Async variant of verify."""
        raise NotImplementedError
