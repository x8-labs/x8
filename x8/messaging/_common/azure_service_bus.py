from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.servicebus import (
    ServiceBusMessage,
    ServiceBusMessageBatch,
    ServiceBusReceivedMessage,
)
from azure.servicebus.management import (
    QueueProperties,
    QueueRuntimeProperties,
    SubscriptionProperties,
    SubscriptionRuntimeProperties,
    TopicProperties,
    TopicRuntimeProperties,
)
from x8._common.azure_provider import AzureProvider
from x8.core import Context, DataModel, NCall, Operation, Response
from x8.core.exceptions import BadRequestError, ConflictError, NotFoundError

from ._models import (
    DEFAULT_SUBSCRIPTION_NAME,
    MessageBatch,
    MessageItem,
    MessageKey,
    MessageProperties,
    MessagePullConfig,
    MessagePutConfig,
    MessageValueType,
    MessagingMode,
    MessagingOperation,
    QueueConfig,
    QueueInfo,
    SubscriptionConfig,
    SubscriptionInfo,
    TopicConfig,
    TopicInfo,
)
from ._operation_parser import MessagingOperationParser


class AzureServiceBusBase(AzureProvider):
    mode: MessagingMode

    queue: str | None
    topic: str | None
    subscription: str | None
    fully_qualified_namespace: str | None
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _topic_cache: dict[str, AzureServiceBusTopic]
    _atopic_cache: dict[str, AzureServiceBusTopic]
    _mgmt_client: Any
    _amgmt_client: Any
    _mgmt_client_helper: Any
    _amgmt_client_helper: Any
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        mode: MessagingMode = MessagingMode.QUEUE,
        queue: str | None = None,
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
            mode:
                Service Bus mode ("queue" or "pubsub").
            queue:
                Service Bus queue name.
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
        self.mode = mode
        self.queue = queue
        self.topic = topic
        self.subscription = subscription
        self.fully_qualified_namespace = fully_qualified_namespace
        self.connection_string = connection_string
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._mgmt_client = None
        self._amgmt_client = None
        self._topic_cache = dict()
        self._atopic_cache = dict()
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()

        if self.mode == MessagingMode.QUEUE:
            self.topic = self.queue
            self.subscription = DEFAULT_SUBSCRIPTION_NAME
        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from azure.servicebus import ServiceBusClient

        (self._credential, self._client) = self._init_credential_client(
            ServiceBusClient,
            self._get_credential,
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from azure.servicebus.aio import ServiceBusClient

        (
            self._acredential,
            self._aclient,
        ) = self._init_credential_client(
            ServiceBusClient, self._aget_credential
        )

    def _init_mgmt_client(self) -> None:
        if self._mgmt_client is not None:
            return

        from azure.servicebus.management import ServiceBusAdministrationClient

        self._credential, self._mgmt_client = self._init_credential_client(
            ServiceBusAdministrationClient,
            self._get_credential,
        )

    async def _ainit_mgmt_client(self) -> None:
        if self._amgmt_client is not None:
            return

        from azure.servicebus.aio.management import (
            ServiceBusAdministrationClient,
        )

        (
            self._acredential,
            self._amgmt_client,
        ) = self._init_credential_client(
            ServiceBusAdministrationClient,
            self._aget_credential,
        )

    def _init_credential_client(
        self,
        client_type,
        get_credential,
    ):
        credential = None
        if self.connection_string is not None:
            client = client_type.from_connection_string(
                conn_str=self.connection_string,
                **self.nparams,
            )
        else:
            credential = get_credential()
            client = client_type(
                fully_qualified_namespace=self.fully_qualified_namespace,
                credential=credential,
                **self.nparams,
            )
        return credential, client

    def _get_topic_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        if self.mode == MessagingMode.PUBSUB:
            topic_name = op_parser.get_topic()
            topic_name = topic_name or self.topic or self.__component__.topic
        else:
            topic_name = op_parser.get_queue()
            topic_name = topic_name or self.queue or self.__component__.queue
        return topic_name

    def _get_subscription_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        subscription_name = op_parser.get_subscription()
        subscription_name = (
            subscription_name
            or self.subscription
            or self.__component__.subscription
        )
        return subscription_name

    def _init_sender(self, topic: AzureServiceBusTopic, topic_name: str):
        if topic.sender is None:
            if self.mode == MessagingMode.QUEUE:
                topic.sender = self._client.get_queue_sender(
                    queue_name=topic_name
                )
            elif self.mode == MessagingMode.PUBSUB:
                topic.sender = self._client.get_topic_sender(
                    topic_name=topic_name
                )

    def _ainit_sender(self, topic: AzureServiceBusTopic, topic_name: str):
        if topic.sender is None:
            if self.mode == MessagingMode.QUEUE:
                topic.sender = self._aclient.get_queue_sender(
                    queue_name=topic_name
                )
            elif self.mode == MessagingMode.PUBSUB:
                topic.sender = self._aclient.get_topic_sender(
                    topic_name=topic_name
                )

    def _init_receiver(
        self,
        topic: AzureServiceBusTopic,
        topic_name: str,
        subscription_name: str,
    ):
        if subscription_name not in topic.receivers:
            if self.mode == MessagingMode.QUEUE:
                topic.receivers[subscription_name] = (
                    self._client.get_queue_receiver(queue_name=topic_name)
                )
            elif self.mode == MessagingMode.PUBSUB:
                topic.receivers[subscription_name] = (
                    self._client.get_subscription_receiver(
                        topic_name=topic_name,
                        subscription_name=subscription_name,
                    )
                )
            topic.receiver_helpers[subscription_name] = ReceiverHelper(
                topic.receivers[subscription_name]
            )

    def _ainit_receiver(
        self,
        topic: AzureServiceBusTopic,
        topic_name: str,
        subscription_name: str,
    ):
        if subscription_name not in topic.receivers:
            if self.mode == MessagingMode.QUEUE:
                topic.receivers[subscription_name] = (
                    self._aclient.get_queue_receiver(queue_name=topic_name)
                )
            elif self.mode == MessagingMode.PUBSUB:
                topic.receivers[subscription_name] = (
                    self._aclient.get_subscription_receiver(
                        topic_name=topic_name,
                        subscription_name=subscription_name,
                    )
                )
            topic.receiver_helpers[subscription_name] = AsyncReceiverHelper(
                topic.receivers[subscription_name]
            )

    def _get_topic(
        self, op_parser: MessagingOperationParser
    ) -> AzureServiceBusTopic | None:
        if op_parser.is_resource_op():
            return None
        topic_name = self._get_topic_name(op_parser)
        subscription_name = self._get_subscription_name(op_parser)
        if topic_name is None:
            raise BadRequestError("Topic name must be specified")
        if topic_name in self._topic_cache:
            topic = self._topic_cache[topic_name]
        else:
            topic = AzureServiceBusTopic()
            self._topic_cache[topic_name] = topic
        if op_parser.is_sender_op():
            self._init_sender(topic, topic_name)
        elif op_parser.is_receiver_op():
            if subscription_name is None:
                raise BadRequestError("Subscription name must be specified")
            self._init_receiver(topic, topic_name, subscription_name)
        return topic

    def _aget_topic(
        self, op_parser: MessagingOperationParser
    ) -> AzureServiceBusTopic | None:
        if op_parser.is_resource_op():
            return None
        topic_name = self._get_topic_name(op_parser)
        subscription_name = self._get_subscription_name(op_parser)
        if topic_name is None:
            raise BadRequestError("Topic name must be specified")
        if topic_name in self._atopic_cache:
            topic = self._atopic_cache[topic_name]
        else:
            topic = AzureServiceBusTopic()
            self._atopic_cache[topic_name] = topic
        if op_parser.is_sender_op():
            self._ainit_sender(topic, topic_name)
        elif op_parser.is_receiver_op():
            if subscription_name is None:
                raise BadRequestError("Subscription name must be specified")
            self._ainit_receiver(topic, topic_name, subscription_name)
        return topic

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = MessagingOperationParser(operation)
        topic = None
        mgmt_helper = None
        resource_helper = None
        if op_parser.is_resource_op():
            self._init_mgmt_client()
            mgmt_helper = ManagementHelper(
                self._mgmt_client,
                self._op_converter,
                self._result_converter,
            )
        else:
            topic = self._get_topic(op_parser)
        resource_helper = ResourceHelper(
            self._client,
            self._topic_cache,
            self._mgmt_client,
        )
        ncall = self._get_ncall(
            op_parser,
            topic,
            resource_helper,
            mgmt_helper,
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(nresult, op_parser)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = MessagingOperationParser(operation)
        topic = None
        mgmt_helper = None
        resource_helper = None
        if op_parser.is_resource_op():
            await self._ainit_mgmt_client()
            mgmt_helper = AsyncManagementHelper(
                self._amgmt_client,
                self._op_converter,
                self._result_converter,
            )
        else:
            topic = self._aget_topic(op_parser)
        resource_helper = AsyncResourceHelper(
            self._aclient,
            self._atopic_cache,
            self._amgmt_client,
        )
        ncall = self._get_ncall(
            op_parser,
            topic,
            resource_helper,
            mgmt_helper,
        )
        if ncall is None:
            return await super().__arun__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(nresult, op_parser)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: MessagingOperationParser,
        topic: AzureServiceBusTopic | None,
        resource_helper: Any,
        mgmt_helper: Any,
    ) -> NCall | None:
        if topic is not None:
            sender = topic.sender
            subscription_name = self._get_subscription_name(op_parser)
            if subscription_name in topic.receivers:
                receiver = topic.receivers[subscription_name]
                receiver_helper = topic.receiver_helpers[subscription_name]
        call = None
        nargs = op_parser.get_nargs()
        op_converter = self._op_converter
        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args = op_converter.convert_create_queue(
                self._get_topic_name(op_parser),
                op_parser.get_queue_config(),
            )
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.create_queue,
                args,
                nargs,
                {
                    ResourceExistsError: (
                        ConflictError if where_exists is False else None
                    )
                },
            )
        # UPDATE QUEUE
        elif op_parser.op_equals(MessagingOperation.UPDATE_QUEUE):
            args = {
                "queue_name": self._get_topic_name(op_parser),
                "config": op_parser.get_queue_config(),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.update_queue,
                args,
                None,
                {ResourceNotFoundError: NotFoundError},
            )
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            args = {"queue_name": self._get_topic_name(op_parser)}
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.delete_queue,
                args,
                None,
                {
                    ResourceNotFoundError: (
                        NotFoundError if where_exists is True else None
                    )
                },
            )
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            args = {"queue_name": self._get_topic_name(op_parser)}
            call = NCall(
                mgmt_helper.mgmt_client.get_queue,
                args,
                None,
                {
                    ResourceNotFoundError: None,
                },
            )
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            args = {
                "queue_name": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.get_queue,
                args,
                None,
                {
                    ResourceNotFoundError: NotFoundError,
                },
            )
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            args = {"nargs": nargs}
            call = NCall(mgmt_helper.list_queues, args)
        # CREATE TOPIC
        elif op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            args = op_converter.convert_create_topic(
                self._get_topic_name(op_parser),
                op_parser.get_topic_config(),
            )
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.create_topic,
                args,
                nargs,
                {
                    ResourceExistsError: (
                        ConflictError if where_exists is False else None
                    )
                },
            )
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "config": op_parser.get_topic_config(),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.update_topic,
                args,
                None,
                {ResourceNotFoundError: NotFoundError},
            )
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            args = {"topic_name": self._get_topic_name(op_parser)}
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.delete_topic,
                args,
                None,
                {
                    ResourceNotFoundError: (
                        NotFoundError if where_exists is True else None
                    )
                },
            )
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            args = {"nargs": nargs}
            call = NCall(mgmt_helper.list_topics, args)
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            args = {"topic_name": self._get_topic_name(op_parser)}
            call = NCall(
                mgmt_helper.mgmt_client.get_topic,
                args,
                None,
                {
                    ResourceNotFoundError: None,
                },
            )
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.get_topic,
                args,
                None,
                {
                    ResourceNotFoundError: NotFoundError,
                },
            )
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            args = op_converter.convert_create_subscription(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_subscription_config(),
            )
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.create_subscription,
                args,
                nargs,
                {
                    ResourceExistsError: (
                        ConflictError if where_exists is False else None
                    )
                },
            )
        # UPDATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.UPDATE_SUBSCRIPTION):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "subscription_name": self._get_subscription_name(op_parser),
                "config": op_parser.get_subscription_config(),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.update_subscription,
                args,
                None,
                {ResourceNotFoundError: NotFoundError},
            )
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "subscription_name": self._get_subscription_name(op_parser),
            }
            where_exists = op_parser.get_where_exists()
            call = NCall(
                mgmt_helper.mgmt_client.delete_subscription,
                args,
                None,
                {
                    ResourceNotFoundError: (
                        NotFoundError if where_exists is True else None
                    )
                },
            )
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(mgmt_helper.list_subscriptions, args)
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "subscription_name": self._get_subscription_name(op_parser),
            }
            call = NCall(
                mgmt_helper.mgmt_client.get_subscription,
                args,
                None,
                {
                    ResourceNotFoundError: None,
                },
            )
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            args = {
                "topic_name": self._get_topic_name(op_parser),
                "subscription_name": self._get_subscription_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                mgmt_helper.get_subscription,
                args,
                None,
                {
                    ResourceNotFoundError: NotFoundError,
                },
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = op_converter.convert_put(
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                sender.send_messages,
                args,
                nargs,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = op_converter.convert_batch(
                op_parser.get_batch(),
            )
            call = NCall(
                sender.send_messages,
                args,
                nargs,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = op_converter.convert_pull(
                op_parser.get_pull_config(),
            )
            call = NCall(
                receiver.receive_messages,
                args,
                nargs,
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = op_converter.convert_key(op_parser.get_key())
            call = NCall(
                receiver.complete_message,
                args,
                nargs,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = op_converter.convert_key(op_parser.get_key())
            call = NCall(
                receiver.abandon_message,
                args,
                nargs,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = op_converter.convert_key(op_parser.get_key())
            call = NCall(
                receiver.renew_message_lock,
                args,
                nargs,
            )
            pass
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            args = {
                "config": op_parser.get_pull_config(),
                "nargs": nargs,
            }
            call = NCall(
                receiver_helper.purge,
                args,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {
                "topic_name": op_parser.get_topic() or op_parser.get_queue(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.close, args)
        return call

    def _convert_nresult(
        self,
        nresult: Any,
        op_parser: MessagingOperationParser,
    ) -> Any:
        result_converter = self._result_converter
        result: Any = None
        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            result = None
        # UPDATE QUEUE
        elif op_parser.op_equals(MessagingOperation.UPDATE_QUEUE):
            result = None
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            result = None
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            result = nresult
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            result = True if nresult else False
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            result = nresult
        # CREATE TOPIC
        elif op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            result = None
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            result = None
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            result = None
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            result = nresult
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            result = True if nresult else False
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            result = nresult
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            result = None
        # UPDATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.UPDATE_SUBSCRIPTION):
            result = None
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            result = None
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            result = nresult
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            result = True if nresult else False
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            result = nresult
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            result = None
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            result = None
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            result = result_converter.convert_pull(nresult)
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            result = None
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            result = None
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            result = MessageItem(key=op_parser.get_key())
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            result = None
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            result = None
        return result


class ManagementHelper:
    mgmt_client: Any
    op_converter: OperationConverter
    result_converter: ResultConverter

    def __init__(
        self,
        mgmt_client: Any,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
    ):
        self.mgmt_client = mgmt_client
        self.op_converter = op_converter
        self.result_converter = result_converter

    def update_queue(
        self,
        queue_name: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        queue_properties: QueueProperties = NCall(
            self.mgmt_client.get_queue,
            {"queue_name": queue_name},
            nargs,
        ).invoke()
        queue_properties = self.op_converter.convert_update_queue(
            queue_properties, config
        )
        NCall(
            self.mgmt_client.update_queue,
            {"queue": queue_properties},
            nargs,
        ).invoke()

    def get_queue(self, queue_name: str, nargs: Any) -> QueueInfo:
        queue_properties: QueueProperties = NCall(
            self.mgmt_client.get_queue,
            {"queue_name": queue_name},
            nargs,
        ).invoke()
        runtime_queue_properties: QueueRuntimeProperties = NCall(
            self.mgmt_client.get_queue_runtime_properties,
            {"queue_name": queue_name},
            nargs,
        ).invoke()
        return self.result_converter.convert_get_queue(
            queue_properties=queue_properties,
            runtime_queue_properties=runtime_queue_properties,
        )

    def list_queues(self, nargs: Any) -> list[str]:
        queues = NCall(self.mgmt_client.list_queues, None, nargs).invoke()
        return [queue.name for queue in queues]

    def update_topic(
        self,
        topic_name: str,
        config: TopicConfig | None,
        nargs: Any,
    ) -> None:
        topic_properties: TopicProperties = NCall(
            self.mgmt_client.get_topic,
            {"topic_name": topic_name},
            nargs,
        ).invoke()
        topic_properties = self.op_converter.convert_update_topic(
            topic_properties, config
        )
        NCall(
            self.mgmt_client.update_topic,
            {"topic": topic_properties},
            nargs,
        ).invoke()

    def get_topic(self, topic_name: str, nargs: Any) -> TopicInfo:
        topic_properties: TopicProperties = NCall(
            self.mgmt_client.get_topic,
            {"topic_name": topic_name},
            nargs,
        ).invoke()
        runtime_topic_properties: TopicRuntimeProperties = NCall(
            self.mgmt_client.get_topic_runtime_properties,
            {"topic_name": topic_name},
            nargs,
        ).invoke()
        return self.result_converter.convert_get_topic(
            topic_properties=topic_properties,
            runtime_topic_properties=runtime_topic_properties,
        )

    def list_topics(self, nargs: Any) -> list[str]:
        topics = NCall(self.mgmt_client.list_topics, None, nargs).invoke()
        return [topic.name for topic in topics]

    def update_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ) -> None:
        subscription_properties: SubscriptionProperties = NCall(
            self.mgmt_client.get_subscription,
            {"topic_name": topic_name, "subscription_name": subscription_name},
            nargs,
        ).invoke()
        subscription_properties = (
            self.op_converter.convert_update_subscription(
                subscription_properties, config
            )
        )
        NCall(
            self.mgmt_client.update_subscription,
            {
                "topic_name": topic_name,
                "subscription": subscription_properties,
            },
            nargs,
        ).invoke()

    def get_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        subscription_properties: SubscriptionProperties = NCall(
            self.mgmt_client.get_subscription,
            {"topic_name": topic_name, "subscription_name": subscription_name},
            nargs,
        ).invoke()
        runtime_subscription_properties: SubscriptionRuntimeProperties = NCall(
            self.mgmt_client.get_subscription_runtime_properties,
            {"topic_name": topic_name, "subscription_name": subscription_name},
            nargs,
        ).invoke()
        return self.result_converter.convert_get_subscription(
            subscription_properties=subscription_properties,
            runtime_subscription_properties=runtime_subscription_properties,
            topic_name=topic_name,
        )

    def list_subscriptions(self, topic_name: str, nargs: Any) -> list[str]:
        subscriptions = NCall(
            self.mgmt_client.list_subscriptions,
            {"topic_name": topic_name},
            nargs,
        ).invoke()
        return [subscription.name for subscription in subscriptions]


class AsyncManagementHelper:
    mgmt_client: Any
    op_converter: OperationConverter
    result_converter: ResultConverter

    def __init__(
        self,
        mgmt_client: Any,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
    ):
        self.mgmt_client = mgmt_client
        self.op_converter = op_converter
        self.result_converter = result_converter

    async def update_queue(
        self,
        queue_name: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        queue_properties: QueueProperties = await NCall(
            self.mgmt_client.get_queue,
            {"queue_name": queue_name},
            nargs,
        ).ainvoke()
        queue_properties = self.op_converter.convert_update_queue(
            queue_properties, config
        )
        await NCall(
            self.mgmt_client.update_queue,
            {"queue": queue_properties},
            nargs,
        ).ainvoke()

    async def get_queue(self, queue_name: str, nargs: Any) -> QueueInfo:
        queue_properties: QueueProperties = await NCall(
            self.mgmt_client.get_queue,
            {"queue_name": queue_name},
            nargs,
        ).ainvoke()
        runtime_queue_properties: QueueRuntimeProperties = await NCall(
            self.mgmt_client.get_queue_runtime_properties,
            {"queue_name": queue_name},
            nargs,
        ).ainvoke()
        return self.result_converter.convert_get_queue(
            queue_properties=queue_properties,
            runtime_queue_properties=runtime_queue_properties,
        )

    async def list_queues(self, nargs: Any) -> list[str]:
        queues = NCall(self.mgmt_client.list_queues, None, nargs).invoke()
        return [queue.name async for queue in queues]

    async def update_topic(
        self,
        topic_name: str,
        config: TopicConfig | None,
        nargs: Any,
    ) -> None:
        topic_properties: TopicProperties = await NCall(
            self.mgmt_client.get_topic,
            {"topic_name": topic_name},
            nargs,
        ).ainvoke()
        topic_properties = self.op_converter.convert_update_topic(
            topic_properties, config
        )
        await NCall(
            self.mgmt_client.update_topic,
            {"topic": topic_properties},
            nargs,
        ).ainvoke()

    async def get_topic(self, topic_name: str, nargs: Any) -> TopicInfo:
        topic_properties: TopicProperties = await NCall(
            self.mgmt_client.get_topic,
            {"topic_name": topic_name},
            nargs,
        ).ainvoke()
        runtime_topic_properties: TopicRuntimeProperties = await NCall(
            self.mgmt_client.get_topic_runtime_properties,
            {"topic_name": topic_name},
            nargs,
        ).ainvoke()
        return self.result_converter.convert_get_topic(
            topic_properties=topic_properties,
            runtime_topic_properties=runtime_topic_properties,
        )

    async def list_topics(self, nargs: Any) -> list[str]:
        topics = NCall(self.mgmt_client.list_topics, None, nargs).invoke()
        return [topic.name async for topic in topics]

    async def update_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ) -> None:
        subscription_properties: SubscriptionProperties = await NCall(
            self.mgmt_client.get_subscription,
            {"topic_name": topic_name, "subscription_name": subscription_name},
            nargs,
        ).ainvoke()
        subscription_properties = (
            self.op_converter.convert_update_subscription(
                subscription_properties, config
            )
        )
        await NCall(
            self.mgmt_client.update_subscription,
            {
                "topic_name": topic_name,
                "subscription": subscription_properties,
            },
            nargs,
        ).ainvoke()

    async def get_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        subscription_properties: SubscriptionProperties = await NCall(
            self.mgmt_client.get_subscription,
            {"topic_name": topic_name, "subscription_name": subscription_name},
            nargs,
        ).ainvoke()
        runtime_subscription_properties: SubscriptionRuntimeProperties = (
            await NCall(
                self.mgmt_client.get_subscription_runtime_properties,
                {
                    "topic_name": topic_name,
                    "subscription_name": subscription_name,
                },
                nargs,
            ).ainvoke()
        )
        return self.result_converter.convert_get_subscription(
            subscription_properties=subscription_properties,
            runtime_subscription_properties=runtime_subscription_properties,
            topic_name=topic_name,
        )

    async def list_subscriptions(
        self, topic_name: str, nargs: Any
    ) -> list[str]:
        subscriptions = NCall(
            self.mgmt_client.list_subscriptions,
            {"topic_name": topic_name},
            nargs,
        ).invoke()
        return [subscription.name async for subscription in subscriptions]


class ResourceHelper:
    client: Any
    topic_cache: dict[str, AzureServiceBusTopic]
    mgmt_client: Any

    def __init__(self, client: Any, topic_cache: dict, mgmt_client: Any):
        self.client = client
        self.topic_cache = topic_cache

    def close(self, topic_name: str | None, nargs: Any) -> Any:
        if topic_name is not None and topic_name in self.topic_cache:
            self.topic_cache.pop(topic_name)
        else:
            self.topic_cache.clear()


class AsyncResourceHelper:
    client: Any
    topic_cache: dict[str, AzureServiceBusTopic]
    mgmt_client: Any

    def __init__(self, client: Any, topic_cache: dict, mgmt_client: Any):
        self.client = client
        self.topic_cache = topic_cache
        self.mgmt_client = mgmt_client

    async def close(self, topic_name: str | None, nargs: Any) -> None:
        if topic_name is not None and topic_name in self.topic_cache:
            topic = self.topic_cache[topic_name]
            if topic.sender is not None:
                await NCall(
                    topic.sender.close,
                    None,
                    nargs,
                ).ainvoke()
            for receiver in topic.receivers.values():
                await NCall(
                    receiver.close,
                    None,
                    nargs,
                ).ainvoke()
            self.topic_cache.pop(topic_name)
        else:
            for key, topic in self.topic_cache.items():
                if topic.sender is not None:
                    await NCall(
                        topic.sender.close,
                        None,
                        nargs,
                    ).ainvoke()
                for receiver in topic.receivers.values():
                    await NCall(
                        receiver.close,
                        None,
                        nargs,
                    ).ainvoke()
            await NCall(
                self.client.close,
                None,
                nargs,
            ).ainvoke()
            self.topic_cache.clear()
        if self.mgmt_client is not None:
            await NCall(
                self.mgmt_client.close,
                None,
                nargs,
            ).ainvoke()


class ReceiverHelper:
    receiver: Any

    def __init__(self, receiver: Any):
        self.receiver = receiver

    def purge(
        self,
        config: MessagePullConfig | None,
        nargs: Any,
    ) -> None:
        while True:
            max_message_count = 10
            max_wait_time = None
            if config is not None and config.max_count is not None:
                max_message_count = config.max_count
            if config is not None and config.max_wait_time is not None:
                max_wait_time = config.max_wait_time
            messages = NCall(
                self.receiver.receive_messages,
                {
                    "max_message_count": max_message_count,
                    "max_wait_time": max_wait_time,
                },
                nargs,
            ).invoke()
            if len(messages) == 0:
                break
            for message in messages:
                NCall(
                    self.receiver.complete_message, {"message": message}
                ).invoke()


class AsyncReceiverHelper:
    receiver: Any

    def __init__(self, receiver: Any):
        self.receiver = receiver

    async def purge(
        self,
        config: MessagePullConfig | None,
        nargs: Any,
    ) -> None:
        while True:
            max_message_count = 10
            max_wait_time = None
            if config is not None and config.max_count is not None:
                max_message_count = config.max_count
            if config is not None and config.max_wait_time is not None:
                max_wait_time = config.max_wait_time
            messages = await NCall(
                self.receiver.receive_messages,
                {
                    "max_message_count": max_message_count,
                    "max_wait_time": max_wait_time,
                },
                nargs,
            ).ainvoke()
            if len(messages) == 0:
                break
            tasks = []
            for message in messages:
                tasks.append(
                    NCall(
                        self.receiver.complete_message, {"message": message}
                    ).invoke(),
                )
            import asyncio

            await asyncio.gather(*tasks)


class OperationConverter:
    def convert_update_queue(
        self,
        queue_properties: QueueProperties,
        config: QueueConfig | None,
    ) -> QueueProperties:
        if config is not None:
            if config.visibility_timeout is not None:
                queue_properties.lock_duration = timedelta(
                    seconds=config.visibility_timeout
                )
            if config.ttl is not None:
                queue_properties.default_message_time_to_live = timedelta(
                    seconds=config.ttl
                )
            if config.max_delivery_count is not None:
                queue_properties.max_delivery_count = config.max_delivery_count

            if config.dlq_nref is not None:
                queue_properties.forward_dead_lettered_messages_to = (
                    config.dlq_nref
                )
            if config.nconfig:
                for key, value in config.nconfig.items():
                    if hasattr(queue_properties, key):
                        setattr(queue_properties, key, value)
        return queue_properties

    def convert_update_topic(
        self,
        topic_properties: TopicProperties,
        config: TopicConfig | None,
    ) -> TopicProperties:
        if config is not None:
            if config.ttl is not None:
                topic_properties.default_message_time_to_live = timedelta(
                    seconds=config.ttl
                )
            if config.nconfig:
                for key, value in config.nconfig.items():
                    if hasattr(topic_properties, key):
                        setattr(topic_properties, key, value)
        return topic_properties

    def convert_update_subscription(
        self,
        subscription_properties: SubscriptionProperties,
        config: SubscriptionConfig | None,
    ) -> SubscriptionProperties:
        if config is not None:
            if config.visibility_timeout is not None:
                subscription_properties.lock_duration = timedelta(
                    seconds=config.visibility_timeout
                )
            if config.ttl is not None:
                subscription_properties.default_message_time_to_live = (
                    timedelta(seconds=config.ttl)
                )
            if config.max_delivery_count is not None:
                subscription_properties.max_delivery_count = (
                    config.max_delivery_count
                )

            if config.dlq_nref is not None:
                subscription_properties.forward_dead_lettered_messages_to = (
                    config.dlq_nref
                )
            if config.nconfig:
                for key, value in config.nconfig.items():
                    if hasattr(subscription_properties, key):
                        setattr(subscription_properties, key, value)
        return subscription_properties

    def _convert_queue_config(
        self,
        config: QueueConfig | None,
    ) -> dict:
        args: dict = {}

        if config:
            if config.visibility_timeout is not None:
                args["lock_duration"] = timedelta(
                    seconds=config.visibility_timeout
                )
            if config.ttl is not None:
                args["default_message_time_to_live"] = timedelta(
                    seconds=config.ttl
                )
            if config.max_delivery_count is not None:
                args["max_delivery_count"] = config.max_delivery_count

            if config.dlq_nref is not None:
                args["forward_dead_lettered_messages_to"] = config.dlq_nref

            if config.nconfig:
                args.update(config.nconfig)

        return args

    def _convert_topic_config(
        self,
        config: TopicConfig | None,
    ) -> dict:
        args: dict = {}
        if config:
            if config.ttl is not None:
                args["default_message_time_to_live"] = timedelta(
                    seconds=config.ttl
                )
            if config.nconfig:
                args.update(config.nconfig)
        return args

    def _convert_subscription_config(
        self,
        config: SubscriptionConfig | None,
    ) -> dict:
        args: dict = {}
        if config:
            if config.visibility_timeout is not None:
                args["lock_duration"] = timedelta(
                    seconds=config.visibility_timeout
                )
            if config.ttl is not None:
                args["default_message_time_to_live"] = timedelta(
                    seconds=config.ttl
                )
            if config.max_delivery_count is not None:
                args["max_delivery_count"] = config.max_delivery_count

            if config.dlq_nref is not None:
                args["forward_dead_lettered_messages_to"] = config.dlq_nref

            if config.nconfig:
                args.update(config.nconfig)
        return args

    def convert_create_queue(
        self,
        queue_name: str | None,
        config: QueueConfig | None,
    ):
        args: dict = {
            "queue_name": queue_name,
        }
        if config:
            args.update(self._convert_queue_config(config))
        return args

    def convert_create_topic(
        self,
        topic_name: str | None,
        config: TopicConfig | None,
    ):
        args: dict = {
            "topic_name": topic_name,
        }
        if config:
            args.update(self._convert_topic_config(config))
        return args

    def convert_create_subscription(
        self,
        topic_name: str | None,
        subscription_name: str | None,
        config: SubscriptionConfig | None,
    ):
        args: dict = {
            "topic_name": topic_name,
            "subscription_name": subscription_name,
        }
        if config:
            args.update(self._convert_subscription_config(config))
        return args

    def convert_put(
        self,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict:
        message_id = None
        content_type = None
        scheduled_time: datetime | None = None
        group_id = None
        body: str | bytes | None = None
        if properties is not None:
            message_id = properties.message_id
            content_type = properties.content_type
            group_id = properties.group_id
        if isinstance(value, str):
            body = value
            content_type = "text/plain"
        elif isinstance(value, bytes):
            body = value
            content_type = "application/octet-stream"
        elif isinstance(value, dict):
            body = json.dumps(value)
            content_type = "application/json"
        elif isinstance(value, DataModel):
            body = value.to_json()
            content_type = "application/json"
        else:
            raise BadRequestError("Message type not supported")
        if config is not None and config.delay is not None:
            scheduled_time = datetime.now(timezone.utc) + timedelta(
                seconds=config.delay
            )
        return {
            "message": ServiceBusMessage(
                body=body,
                message_id=message_id,
                content_type=content_type,
                session_id=group_id,
                scheduled_enqueue_time_utc=scheduled_time,
                application_properties=metadata,
            )
        }

    def convert_batch(self, batch: MessageBatch) -> dict:
        sb_batch = ServiceBusMessageBatch()
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            sb_batch.add_message(
                message=self.convert_put(
                    op_parser.get_value(),
                    op_parser.get_metadata(),
                    op_parser.get_properties(),
                    op_parser.get_put_config(),
                )["message"]
            )
        return {"message": sb_batch}

    def convert_pull(self, config: MessagePullConfig | None) -> dict:
        max_message_count = 1
        max_wait_time = None
        if config is not None and config.max_count is not None:
            max_message_count = config.max_count
        if config is not None and config.max_wait_time is not None:
            max_wait_time = config.max_wait_time
        return {
            "max_message_count": max_message_count,
            "max_wait_time": max_wait_time,
        }

    def convert_key(self, key: MessageKey) -> dict:
        return {"message": key.nref}


class ResultConverter:
    def _convert_key(self, message: ServiceBusReceivedMessage) -> MessageKey:
        return MessageKey(nref=message)

    def _convert_value(
        self, message: ServiceBusReceivedMessage
    ) -> MessageValueType:
        if message.content_type == "application/octet-stream":
            return b"".join(message.raw_amqp_message.body)
        elif message.content_type == "application/json":
            return json.loads(str(message))
        return str(message)

    def _convert_metadata(
        self, message: ServiceBusReceivedMessage
    ) -> dict | None:
        if message.application_properties is None:
            return None
        metadata = {}
        for key, value in message.application_properties.items():
            if isinstance(key, bytes):
                k = key.decode("utf-8")
            else:
                k = key
            if isinstance(value, bytes):
                v: Any = value.decode("utf-8")
            else:
                v = value
            metadata[k] = v
        return metadata

    def _convert_properties(
        self,
        message: ServiceBusReceivedMessage,
    ) -> MessageProperties:
        return MessageProperties(
            message_id=message.message_id,
            content_type=message.content_type,
            group_id=message.session_id,
            enqueued_time=(
                message.enqueued_time_utc.timestamp()
                if message.enqueued_time_utc
                else None
            ),
            delivery_count=message.delivery_count,
        )

    def _convert_message(
        self,
        message: ServiceBusReceivedMessage,
    ) -> MessageItem:
        return MessageItem(
            key=self._convert_key(message=message),
            value=self._convert_value(message=message),
            metadata=self._convert_metadata(message=message),
            properties=self._convert_properties(message=message),
        )

    def convert_pull(
        self, messages: list[ServiceBusReceivedMessage]
    ) -> list[MessageItem]:
        result = []
        for message in messages:
            result.append(self._convert_message(message))
        return result

    def convert_get_queue(
        self,
        queue_properties: QueueProperties,
        runtime_queue_properties: QueueRuntimeProperties,
    ) -> QueueInfo:
        rqp = runtime_queue_properties
        visibility_timeout = None
        ttl = None
        qp_ld = queue_properties.lock_duration
        if qp_ld is not None:
            if isinstance(qp_ld, timedelta):
                visibility_timeout = qp_ld.total_seconds()
            else:
                visibility_timeout = None
        qp_ttl = queue_properties.default_message_time_to_live
        if qp_ttl is not None:
            if isinstance(qp_ttl, timedelta):
                ttl = qp_ttl.total_seconds()
            else:
                ttl = None
        config = QueueConfig(
            visibility_timeout=visibility_timeout,
            ttl=ttl,
            max_delivery_count=queue_properties.max_delivery_count,
            dlq_nref=queue_properties.forward_dead_lettered_messages_to,
            nconfig=queue_properties.__dict__,
        )
        queue_info = QueueInfo(
            name=queue_properties.name,
            active_message_count=rqp.active_message_count,
            inflight_message_count=(
                (rqp.total_message_count or 0)
                - (rqp.active_message_count or 0)
            ),
            scheduled_message_count=rqp.scheduled_message_count,
            config=config,
            nref=queue_properties.name,
        )
        return queue_info

    def convert_get_topic(
        self,
        topic_properties: TopicProperties,
        runtime_topic_properties: TopicRuntimeProperties,
    ) -> TopicInfo:
        rtp = runtime_topic_properties
        ttl = None
        tp_ttl = topic_properties.default_message_time_to_live
        if tp_ttl is not None:
            if isinstance(tp_ttl, timedelta):
                ttl = tp_ttl.total_seconds()
            else:
                ttl = None
        config = TopicConfig(
            ttl=ttl,
            nconfig=topic_properties.__dict__,
        )
        topic_info = TopicInfo(
            name=topic_properties.name,
            subscription_count=rtp.subscription_count,
            scheduled_message_count=rtp.scheduled_message_count,
            config=config,
            nref=topic_properties.name,
        )
        return topic_info

    def convert_get_subscription(
        self,
        subscription_properties: SubscriptionProperties,
        runtime_subscription_properties: SubscriptionRuntimeProperties,
        topic_name: str,
    ) -> SubscriptionInfo:
        rsp = runtime_subscription_properties
        visibility_timeout = None
        ttl = None
        sp_ld = subscription_properties.lock_duration
        if sp_ld is not None:
            if isinstance(sp_ld, timedelta):
                visibility_timeout = sp_ld.total_seconds()
            else:
                visibility_timeout = None
        sp_ttl = subscription_properties.default_message_time_to_live
        if sp_ttl is not None:
            if isinstance(sp_ttl, timedelta):
                ttl = sp_ttl.total_seconds()
            else:
                ttl = None
        config = SubscriptionConfig(
            visibility_timeout=visibility_timeout,
            ttl=ttl,
            max_delivery_count=subscription_properties.max_delivery_count,
            dlq_nref=subscription_properties.forward_dead_lettered_messages_to,
            nconfig=subscription_properties.__dict__,
        )
        subscription_info = SubscriptionInfo(
            name=subscription_properties.name,
            topic=topic_name,
            active_message_count=rsp.active_message_count,
            inflight_message_count=(
                (rsp.total_message_count or 0)
                - (rsp.active_message_count or 0)
            ),
            config=config,
            nref=subscription_properties.name,
        )
        return subscription_info


class AzureServiceBusTopic:
    sender: Any
    receivers: dict[str, Any]
    receiver_helpers: dict[str, Any]

    def __init__(self):
        self.sender = None
        self.receivers = {}
        self.receiver_helpers = {}


"""
    This is a bug in the client. The last message will not be received.
    servicebus_client = ServiceBusClient.from_connection_string(
        conn_str=NAMESPACE_CONNECTION_STR,
        logging_enable=True)
    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
    await sender.send_messages(ServiceBusMessage("Message 1"))
    while True:
        messages = await receiver.receive_messages(max_wait_time=2, max_message_count=10)
        if len(messages) == 0:
            break
        for message in messages:
            print(f"Completing - {message.message}")
            await receiver.complete_message(message)
    await sender.send_messages(ServiceBusMessage("Message 2"))
    messages = await receiver.receive_messages(max_message_count=1)
    await receiver.complete_message(messages[0])
    print(f"Completing - {messages[0].message}")
    await sender.send_messages(ServiceBusMessage("Message 3"))
    messages = await receiver.receive_messages(max_message_count=1)
    await receiver.complete_message(messages[0])
    print(f"Completing - {messages[0].message}")
    await sender.send_messages(ServiceBusMessage("Message 4"))
    messages = await receiver.receive_messages(max_message_count=1)
    await receiver.complete_message(messages[0])
    print(f"Completing - {messages[0].message}")
    await servicebus_client.close()
"""  # noqa
