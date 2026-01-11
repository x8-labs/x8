__all__ = ["AzureServiceBus"]

from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.azure_service_bus import AzureServiceBusBase


class AzureServiceBus(AzureServiceBusBase):
    def __init__(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        fully_qualified_namespace: str | None = None,
        connection_string: str | None = None,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            topic:
                Service Bus topic name.
            subscription:
                Service Bus subscription name.
            fully_qualified_namespace:
                The fully qualified host name for the Service Bus namespace.
                The namespace format is:
                    `<yournamespace>.servicebus.windows.net`.
            connection_string:
                The connection string of a Service Bus.
            credential_type:
                Azure credential type.
            tenant_id:
                Azure tenant id for client_secret credential type.
            client_id:
                Azure client id for client_secret credential type.
            client_secret:
                Azure client secret for client_secret credential type.
            certificate_path:
                Certificate path for certificate credential type.
            nparams:
                Native parameters to Service Bus client.
        """
        super().__init__(
            mode=MessagingMode.PUBSUB,
            topic=topic,
            subscription=subscription,
            fully_qualified_namespace=fully_qualified_namespace,
            connection_string=connection_string,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            nparams=nparams,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return True
