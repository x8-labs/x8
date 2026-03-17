from __future__ import annotations

from typing import Any, NoReturn

from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from x8.core import Provider, Response
from x8.core.exceptions import BadRequestError, UnauthorizedError

from .._models import (
    MessageChannel,
    MessageContent,
    MessageMedia,
    MessageSendResult,
    VerificationCheckResult,
    VerificationStartResult,
)

__all__ = ["Twilio"]


class Twilio(Provider):
    account_sid: str | None
    auth_token: str | None
    from_: str | None
    messaging_service_sid: str | None
    verify_service_sid: str | None
    nparams: dict[str, Any]

    _client: Client | None

    def __init__(
        self,
        account_sid: str | None = None,
        auth_token: str | None = None,
        from_: str | None = None,
        messaging_service_sid: str | None = None,
        verify_service_sid: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            account_sid:
                Twilio account SID.
            auth_token:
                Twilio auth token.
            from_:
                Default sender (phone number or channel address).
            messaging_service_sid:
                Default messaging service SID.
            verify_service_sid:
                Twilio Verify service SID.
            nparams:
                Native parameters for Twilio client.
        """
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_ = from_
        self.messaging_service_sid = messaging_service_sid
        self.verify_service_sid = verify_service_sid
        self.nparams = nparams or {}
        self._client = None
        super().__init__(**kwargs)

    def __setup__(self, context=None) -> None:
        if self._client is not None:
            return
        self._client = Client(
            username=self.account_sid,
            password=self.auth_token,
            **self.nparams,
        )

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
        self.__setup__()
        channel_name, normalized_to = self._normalize_channel_and_address(
            channel,
            to,
        )
        message_content = self._as_content(content)
        body = message_content.text
        media_urls = self._collect_media_urls(media, message_content.media)
        args: dict[str, Any] = {"to": normalized_to}
        if body is not None:
            args["body"] = body
        if media_urls:
            args["media_url"] = media_urls
        if "body" not in args and "media_url" not in args:
            raise BadRequestError(
                "Message content requires text or at least one media item"
            )

        resolved_msg_service_sid = (
            messaging_service_sid
            or self.messaging_service_sid
            or self.__component__.messaging_service_sid
        )
        if resolved_msg_service_sid is not None:
            args["messaging_service_sid"] = resolved_msg_service_sid
        else:
            sender = from_ or self.from_ or self.__component__.from_
            sender = self._apply_channel_prefix(sender, channel_name)
            if sender is not None:
                args["from_"] = sender
        if status_callback is not None:
            args["status_callback"] = status_callback

        if nconfig:
            args.update(nconfig)

        try:
            assert self._client is not None
            result = self._client.messages.create(**args)
        except TwilioRestException as error:
            self._raise_twilio_error(error)

        return Response(
            result=MessageSendResult(
                id=result.sid,
                status=result.status,
                channel=channel_name,
                to=result.to,
                from_=getattr(result, "from_", None),
                provider="twilio",
                price=result.price,
                currency=result.price_unit,
                info=result.__dict__,
            )
        )

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
        self.__setup__()
        service_sid = self.verify_service_sid
        if service_sid is None:
            raise BadRequestError("Twilio Verify service SID is required")

        channel_name, normalized_to = self._normalize_channel_and_address(
            channel,
            to,
        )
        verify_channel = "whatsapp" if channel_name == "whatsapp" else "sms"

        args: dict[str, Any] = {
            "to": normalized_to,
            "channel": verify_channel,
        }
        if locale is not None:
            args["locale"] = locale
        if custom_friendly_name is not None:
            args["custom_friendly_name"] = custom_friendly_name
        if code_length is not None:
            args["channel_configuration"] = {"code_length": code_length}
        if nconfig:
            args.update(nconfig)

        try:
            assert self._client is not None
            result = self._client.verify.v2.services(
                service_sid
            ).verifications.create(**args)
        except TwilioRestException as error:
            self._raise_twilio_error(error)

        return Response(
            result=VerificationStartResult(
                id=result.sid,
                status=result.status,
                channel=channel_name,
                to=result.to,
                provider="twilio",
                info=result.__dict__,
            )
        )

    def verify(
        self,
        to: str,
        code: str,
        *,
        channel: MessageChannel | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VerificationCheckResult]:
        self.__setup__()
        service_sid = self.verify_service_sid
        if service_sid is None:
            raise BadRequestError("Twilio Verify service SID is required")

        _, normalized_to = self._normalize_channel_and_address(channel, to)
        args: dict[str, Any] = {
            "to": normalized_to,
            "code": code,
        }
        if nconfig:
            args.update(nconfig)

        try:
            assert self._client is not None
            result = self._client.verify.v2.services(
                service_sid
            ).verification_checks.create(**args)
        except TwilioRestException as error:
            self._raise_twilio_error(error)

        return Response(
            result=VerificationCheckResult(
                id=result.sid,
                status=result.status,
                approved=getattr(result, "valid", None),
                valid=getattr(result, "valid", None),
                to=result.to,
                provider="twilio",
                info=result.__dict__,
            )
        )

    def _collect_media_urls(
        self,
        media: list[dict | MessageMedia] | None,
        content_media: list[MessageMedia] | None,
    ) -> list[str]:
        media_urls: list[str] = []
        for item in media or []:
            media_item = (
                MessageMedia.from_dict(item)
                if isinstance(item, dict)
                else item
            )
            media_urls.append(media_item.url)
        for item in content_media or []:
            media_urls.append(item.url)
        return media_urls

    def _as_content(self, content: str | MessageContent) -> MessageContent:
        if isinstance(content, str):
            return MessageContent(text=content)
        return content

    def _normalize_channel_and_address(
        self,
        channel: MessageChannel | None,
        address: str,
    ) -> tuple[MessageChannel, str]:
        if address.startswith("whatsapp:"):
            return "whatsapp", address
        if channel == "whatsapp":
            return "whatsapp", f"whatsapp:{address}"
        return channel or "sms", address

    def _apply_channel_prefix(
        self,
        address: str | None,
        channel: MessageChannel,
    ) -> str | None:
        if address is None:
            return None
        if channel == "whatsapp" and not address.startswith("whatsapp:"):
            return f"whatsapp:{address}"
        return address

    def _raise_twilio_error(self, error: TwilioRestException) -> NoReturn:
        message = (
            f"Twilio API error: {error.msg}"
            if error.msg
            else "Twilio API error"
        )
        if error.status == 401:
            raise UnauthorizedError(message) from error
        raise BadRequestError(message) from error
