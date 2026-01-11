__all__ = ["GooglePubSub"]

from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.google_pubsub import GooglePubSubBase

from .._feature import PubSubFeature


class GooglePubSub(GooglePubSubBase):
    def __init__(
        self,
        project: str | None = None,
        topic: str | None = None,
        subscription: str | None = None,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        enable_message_ordering: bool = True,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            project:
                Google Cloud project name.
            topic:
                Topic name.
            subscription:
                Subscription name.
            service_account_info:
                Service account info in JSON format.
            service_account_file:
                Service account file path.
            access_token:
                Access token for authentication.
            enable_message_ordering:
                Enable message ordering. Defaults to True.
            nparams:
                Native parameters to Pub/Sub client.
        """
        super().__init__(
            project=project,
            mode=MessagingMode.PUBSUB,
            topic=topic,
            subscription=subscription,
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            enable_message_ordering=enable_message_ordering,
            nparams=nparams,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return feature not in [PubSubFeature.BUILTIN_DLQ]
