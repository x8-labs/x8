from __future__ import annotations

import json
from typing import Any

from google.api_core.exceptions import (
    AlreadyExists,
    DeadlineExceeded,
    NotFound,
)
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import PublisherOptions
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1.types import Subscription, Topic

from x8._common.google_provider import GoogleProvider
from x8.core import Context, DataModel, NCall, Operation, Response
from x8.core.exceptions import BadRequestError, ConflictError, NotFoundError

from ._models import (
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


class GooglePubSubBase(GoogleProvider):
    project: str | None
    mode: MessagingMode

    queue: str | None
    topic: str | None
    subscription: str | None
    enable_message_ordering: bool
    nparams: dict[str, Any]

    _publisher_client: PublisherClient
    _subscriber_client: SubscriberClient
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _project: str

    def __init__(
        self,
        project: str | None = None,
        mode: MessagingMode = MessagingMode.QUEUE,
        queue: str | None = None,
        topic: str | None = None,
        subscription: str | None = None,
        enable_message_ordering: bool = True,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            project:
                Google Cloud project ID.
            mode:
                PubSub mode ("queue" or "pubsub").
            queue:
                PubSub queue name (treated as a topic in PubSub).
            topic:
                PubSub topic name.
            subscription:
                PubSub subscription name.
            service_account_info:
                Google Cloud service account info.
            service_account_file:
                Google Cloud service account file path.
            access_token:
                Google Cloud access token.
            enable_message_ordering:
                Enable message ordering.
            nparams:
                Native parameters to PubSub client.
        """
        self.mode = mode
        self.queue = queue
        self.topic = topic
        self.subscription = subscription
        self.project = project
        self.enable_message_ordering = enable_message_ordering
        self.nparams = nparams

        self._publisher_client = None
        self._subscriber_client = None

        if self.mode == MessagingMode.QUEUE:
            self.topic = self.queue
            self.subscription = self.queue
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        GoogleProvider.__init__(
            self,
            service_account_info=self.service_account_info,
            service_account_file=self.service_account_file,
            access_token=self.access_token,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._publisher_client is not None:
            return

        self._project = self.project or self._get_default_project()
        credentials = self._get_credentials()

        self._publisher_client = PublisherClient(
            credentials=credentials,
            publisher_options=PublisherOptions(
                enable_message_ordering=self.enable_message_ordering
            ),
            **self.nparams,
        )
        self._subscriber_client = SubscriberClient(
            credentials=credentials, **self.nparams
        )

    def _get_topic_path(self, op_parser: MessagingOperationParser) -> str:
        topic_name = self._get_topic_name(op_parser)
        if topic_name is None:
            raise BadRequestError("Topic name is required")
        return self._publisher_client.topic_path(self._project, topic_name)

    def _get_subscription_path(
        self, op_parser: MessagingOperationParser
    ) -> str:
        subscription_name = self._get_subscription_name(op_parser)
        if subscription_name is None:
            raise BadRequestError("Subscription name is required")
        return self._subscriber_client.subscription_path(
            self._project, subscription_name
        )

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

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = MessagingOperationParser(operation)
        ncall = self._get_ncall(
            op_parser,
            ClientHelper(
                self._publisher_client,
                self._subscriber_client,
                self._project,
                self._op_converter,
            ),
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

    def _get_ncall(
        self,
        op_parser: MessagingOperationParser,
        client_helper: ClientHelper,
    ) -> NCall | None:
        call = None
        nargs = op_parser.get_nargs()
        op_converter = self._op_converter

        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args: dict[str, Any] = {
                "queue": self._get_topic_name(op_parser),
                "config": op_parser.get_queue_config(),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.create_queue,
                args,
                None,
            )
        # UPDATE QUEUE
        elif op_parser.op_equals(MessagingOperation.UPDATE_QUEUE):
            args = {
                "queue": self._get_topic_name(op_parser),
                "config": op_parser.get_queue_config(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.update_queue,
                args,
                None,
            )
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            args = {
                "queue": self._get_topic_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            where_exists = op_parser.get_where_exists()
            call = NCall(
                client_helper.drop_queue,
                args,
                None,
            )
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            args = {
                "queue": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.has_queue,
                args,
                None,
            )
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            args = {
                "queue": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_queue,
                args,
                None,
            )
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            args = {"project": f"projects/{self._project}"}
            call = NCall(
                self._publisher_client.list_topics,
                args,
                nargs,
            )
        # CREATE TOPIC
        elif op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
                "config": op_parser.get_topic_config(),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.create_topic,
                args,
                None,
            )
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
                "config": op_parser.get_topic_config(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.update_topic,
                args,
                None,
            )
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            where_exists = op_parser.get_where_exists()
            call = NCall(
                client_helper.drop_topic,
                args,
            )
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.has_topic,
                args,
                None,
            )
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_topic,
                args,
                None,
            )
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            args = {"project": f"projects/{self._project}"}
            call = NCall(self._publisher_client.list_topics, args)
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            args = {
                "subscription": self._get_subscription_name(op_parser),
                "topic": self._get_topic_name(op_parser),
                "config": op_parser.get_subscription_config(),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.create_subscription,
                args,
                None,
            )
        # UPDATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.UPDATE_SUBSCRIPTION):
            args = {
                "subscription": self._get_subscription_name(op_parser),
                "topic": self._get_topic_name(op_parser),
                "config": op_parser.get_subscription_config(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.update_subscription,
                args,
                None,
            )
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            args = {
                "subscription": self._get_subscription_path(op_parser),
            }
            where_exists = op_parser.get_where_exists()
            call = NCall(
                self._subscriber_client.delete_subscription,
                args,
                None,
                {NotFound: (NotFoundError if where_exists is True else None)},
            )
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            args = {
                "subscription": self._get_subscription_name(op_parser),
                "topic": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.has_subscription,
                args,
                None,
            )
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            args = {
                "subscription": self._get_subscription_name(op_parser),
                "topic": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_subscription,
                args,
                None,
            )
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            args = {
                "topic": self._get_topic_path(op_parser),
            }
            call = NCall(
                self._publisher_client.list_topic_subscriptions,
                args,
                None,
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = op_converter.convert_put(
                self._get_topic_path(op_parser),
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                self._publisher_client.publish,
                args,
                nargs,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "batch": op_parser.get_batch(),
                "topic_path": self._get_topic_path(op_parser),
            }
            call = NCall(
                client_helper.batch,
                args,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = op_converter.convert_pull(
                self._get_subscription_path(op_parser),
                op_parser.get_pull_config(),
            )
            call = NCall(
                self._subscriber_client.pull,
                args,
                nargs,
                {DeadlineExceeded: None},
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = {
                "subscription": self._get_subscription_path(op_parser),
                "ack_ids": [op_parser.get_key().nref],
            }
            call = NCall(
                self._subscriber_client.acknowledge,
                args,
                nargs,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = {
                "subscription": self._get_subscription_path(op_parser),
                "ack_ids": [op_parser.get_key().nref],
                "ack_deadline_seconds": 0,
            }
            call = NCall(
                self._subscriber_client.modify_ack_deadline,
                args,
                nargs,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = {
                "subscription": self._get_subscription_path(op_parser),
                "ack_ids": [op_parser.get_key().nref],
                "ack_deadline_seconds": op_parser.get_timeout(),
            }
            call = NCall(
                self._subscriber_client.modify_ack_deadline,
                args,
                nargs,
            )
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            args = {
                "subscription_path": self._get_subscription_path(op_parser),
                "config": op_parser.get_pull_config(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.purge,
                args,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {
                "topic": op_parser.get_topic() or op_parser.get_queue(),
                "nargs": nargs,
            }
            call = NCall(client_helper.close, args)
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
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            result = nresult
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            result = nresult
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            result = result_converter.convert_list_topics(nresult)
        # CREATE TOPIC
        elif op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            result = None
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            result = None
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            result = None
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            result = nresult
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            result = nresult
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            result = result_converter.convert_list_topics(nresult)
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            result = None
        # UPDATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.UPDATE_SUBSCRIPTION):
            result = None
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            result = None
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            result = nresult
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            result = nresult
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            result = result_converter.convert_list_subscriptions(nresult)
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


class ClientHelper:
    publisher_client: PublisherClient
    subscriber_client: SubscriberClient
    project: str
    op_converter: OperationConverter

    def __init__(
        self,
        publisher_client: Any,
        subscriber_client: Any,
        project: str,
        op_converter: OperationConverter,
    ):
        self.publisher_client = publisher_client
        self.subscriber_client = subscriber_client
        self.project = project
        self.op_converter = op_converter

    def create_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, queue)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, queue
        )
        try:
            NCall(
                self.publisher_client.create_topic,
                {
                    "name": topic_path,
                },
                nargs,
            ).invoke()
        except AlreadyExists:
            if where_exists is False:
                raise ConflictError

        try:
            args = {
                "name": subscription_path,
                "topic": topic_path,
            }
            config_args = self.op_converter.convert_queue_config(config)
            args.update(config_args)
            NCall(
                self.subscriber_client.create_subscription,
                {"request": args},
                nargs,
            ).invoke()
        except AlreadyExists:
            pass

    def update_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, queue)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, queue
        )
        try:
            args = self.op_converter.convert_queue_config(config)
            subscription = Subscription(
                name=subscription_path, topic=topic_path, **args
            )
            update_mask = FieldMask(paths=args.keys())
            NCall(
                self.subscriber_client.update_subscription,
                {
                    "subscription": subscription,
                    "update_mask": update_mask,
                },
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

    def drop_queue(
        self,
        queue: str,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, queue)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, queue
        )
        try:
            NCall(
                self.subscriber_client.delete_subscription,
                {"subscription": subscription_path},
                nargs,
            ).invoke()
        except NotFound:
            if where_exists is True:
                raise NotFoundError

        try:
            NCall(
                self.publisher_client.delete_topic,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            pass

    def has_queue(
        self,
        queue: str,
        nargs: Any,
    ) -> bool:
        topic_path = self.publisher_client.topic_path(self.project, queue)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, queue
        )
        try:
            NCall(
                self.publisher_client.get_topic,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            return False

        try:
            NCall(
                self.subscriber_client.get_subscription,
                {"subscription": subscription_path},
                nargs,
            ).invoke()
        except NotFound:
            return False
        return True

    def get_queue(self, queue: str, nargs: Any) -> QueueInfo:
        topic_path = self.publisher_client.topic_path(self.project, queue)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, queue
        )
        try:
            subscription: Subscription = NCall(
                self.subscriber_client.get_subscription,
                {"subscription": subscription_path},
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

        return QueueInfo(
            name=queue,
            active_message_count=None,
            inflight_message_count=None,
            scheduled_message_count=None,
            config=QueueConfig(
                visibility_timeout=subscription.ack_deadline_seconds,
                ttl=(
                    subscription.message_retention_duration.seconds
                    if subscription.message_retention_duration
                    else None
                ),
                max_delivery_count=(
                    subscription.dead_letter_policy.max_delivery_attempts
                    if subscription.dead_letter_policy
                    else None
                ),
                dlq_nref=(
                    subscription.dead_letter_policy.dead_letter_topic
                    if subscription.dead_letter_policy
                    else None
                ),
            ),
            nref=topic_path,
        )

    def create_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        try:
            args = {
                "name": topic_path,
            }
            config_args = self.op_converter.convert_topic_config(config)
            args.update(config_args)
            NCall(
                self.publisher_client.create_topic,
                {
                    "request": args,
                },
                nargs,
            ).invoke()
        except AlreadyExists:
            if where_exists is False:
                raise ConflictError

    def update_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        try:
            args = self.op_converter.convert_topic_config(config)
            topic = Topic(
                name=topic_path,
                **args,
            )
            NCall(
                self.publisher_client.update_topic,
                {
                    "topic": topic,
                    "update_mask": FieldMask(paths=list(args.keys())),
                },
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

    def drop_topic(
        self,
        topic: str,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        try:
            subscriptions = NCall(
                self.publisher_client.list_topic_subscriptions,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            subscriptions = []
        try:
            NCall(
                self.publisher_client.delete_topic,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            if where_exists is True:
                raise NotFoundError
        try:
            for subscription in subscriptions:
                NCall(
                    self.subscriber_client.delete_subscription,
                    {"subscription": subscription},
                    nargs,
                ).invoke()
        except NotFound:
            pass

    def has_topic(
        self,
        topic: str,
        nargs: Any,
    ) -> bool:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        try:
            NCall(
                self.publisher_client.get_topic,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            return False
        return True

    def get_topic(self, topic: str, nargs: Any) -> TopicInfo:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        try:
            gtopic: Topic = NCall(
                self.publisher_client.get_topic,
                {"topic": topic_path},
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

        subscriptions = NCall(
            self.publisher_client.list_topic_subscriptions,
            {"topic": topic_path},
            nargs,
        ).invoke()
        sub_count = 0
        for subscription in subscriptions:
            sub_count += 1

        return TopicInfo(
            name=topic,
            config=TopicConfig(
                ttl=(
                    gtopic.message_retention_duration.seconds
                    if gtopic.message_retention_duration
                    else None
                )
            ),
            subscription_count=sub_count,
            nref=topic_path,
        )

    def create_subscription(
        self,
        subscription: str,
        topic: str,
        config: SubscriptionConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription
        )
        try:
            args = {
                "name": subscription_path,
                "topic": topic_path,
            }
            config_args = self.op_converter.convert_subscription_config(config)
            args.update(config_args)
            NCall(
                self.subscriber_client.create_subscription,
                {
                    "request": args,
                },
                nargs,
            ).invoke()
        except AlreadyExists:
            if where_exists is False:
                raise ConflictError

    def update_subscription(
        self,
        subscription: str,
        topic: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ) -> None:
        topic_path = self.publisher_client.topic_path(self.project, topic)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription
        )
        try:
            args = self.op_converter.convert_subscription_config(config)
            subscription = Subscription(
                name=subscription_path,
                topic=topic_path,
                **args,
            )
            NCall(
                self.subscriber_client.update_subscription,
                {
                    "subscription": subscription,
                    "update_mask": FieldMask(paths=list(args.keys())),
                },
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

    def has_subscription(
        self,
        subscription: str,
        topic: str,
        nargs: Any,
    ) -> bool:
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription
        )
        try:
            NCall(
                self.subscriber_client.get_subscription,
                {"subscription": subscription_path},
                nargs,
            ).invoke()
        except NotFound:
            return False
        return True

    def get_subscription(
        self,
        subscription: str,
        topic: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription
        )
        try:
            gsubscription: Subscription = NCall(
                self.subscriber_client.get_subscription,
                {"subscription": subscription_path},
                nargs,
            ).invoke()
        except NotFound:
            raise NotFoundError

        return SubscriptionInfo(
            name=subscription,
            topic=topic,
            config=SubscriptionConfig(
                visibility_timeout=gsubscription.ack_deadline_seconds,
                ttl=(
                    gsubscription.message_retention_duration.seconds
                    if gsubscription.message_retention_duration
                    else None
                ),
                max_delivery_count=(
                    gsubscription.dead_letter_policy.max_delivery_attempts
                    if gsubscription.dead_letter_policy
                    else None
                ),
                dlq_nref=(
                    gsubscription.dead_letter_policy.dead_letter_topic
                    if gsubscription.dead_letter_policy
                    else None
                ),
            ),
            nref=subscription_path,
        )

    def batch(
        self,
        batch: MessageBatch,
        topic_path: str,
    ) -> list:
        message_ids = []
        futures = []
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                topic_path,
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )

            future = self.publisher_client.publish(**args)
            futures.append(future)

        for future in futures:
            message_ids.append(future.result())
        return message_ids

    def purge(
        self,
        subscription_path: str,
        config: MessagePullConfig | None,
        nargs: Any,
    ) -> None:
        max_messages = 10
        if config is not None and config.max_count is not None:
            max_messages = config.max_count

        while True:
            response = NCall(
                self.subscriber_client.pull,
                {
                    "subscription": subscription_path,
                    "max_messages": max_messages,
                },
                nargs,
            ).invoke()

            if not response.received_messages:
                break

            ack_ids = [msg.ack_id for msg in response.received_messages]
            if ack_ids:
                NCall(
                    self.subscriber_client.acknowledge,
                    {"subscription": subscription_path, "ack_ids": ack_ids},
                ).invoke()

    def close(self, topic: str | None, nargs: Any) -> Any:
        if topic is not None:
            return
        self.subscriber_client.close()


class OperationConverter:
    def convert_topic_config(
        self,
        config: TopicConfig | None,
    ) -> dict:
        args: dict = {}
        if config:
            if config.ttl:
                args["message_retention_duration"] = {
                    "seconds": int(config.ttl)
                }
        return args

    def convert_subscription_config(
        self,
        config: SubscriptionConfig | None,
    ) -> dict:
        args: dict = {}
        if config:
            if config.visibility_timeout:
                args["ack_deadline_seconds"] = int(config.visibility_timeout)
            if config.ttl:
                args["message_retention_duration"] = {
                    "seconds": int(config.ttl)
                }
            if config.dlq_nref and config.max_delivery_count:
                args["dead_letter_policy"] = {
                    "dead_letter_topic": config.dlq_nref,
                    "max_delivery_attempts": config.max_delivery_count,
                }
            if config.fifo:
                args["enable_message_ordering"] = True
        return args

    def convert_queue_config(
        self,
        config: QueueConfig | None,
    ) -> dict:
        args: dict = {}
        if config:
            if config.visibility_timeout:
                args["ack_deadline_seconds"] = int(config.visibility_timeout)
            if config.ttl:
                args["message_retention_duration"] = {
                    "seconds": int(config.ttl)
                }
            if config.dlq_nref and config.max_delivery_count:
                args["dead_letter_policy"] = {
                    "dead_letter_topic": config.dlq_nref,
                    "max_delivery_attempts": config.max_delivery_count,
                }
            if config.fifo:
                args["enable_message_ordering"] = True
        return args

    def convert_put(
        self,
        topic_path: str,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict:
        if config is not None and config.delay is not None:
            raise BadRequestError("Delay is not supported for Google PubSub")
        message_id = None
        ordering_key = None

        if properties is not None:
            message_id = properties.message_id
            ordering_key = properties.group_id

        data: bytes | None = None
        attributes = {} if metadata is None else metadata.copy()

        if isinstance(value, str):
            data = value.encode("utf-8")
            attributes["content_type"] = "text/plain"
        elif isinstance(value, bytes):
            data = value
            attributes["content_type"] = "application/octet-stream"
        elif isinstance(value, dict):
            data = json.dumps(value).encode("utf-8")
            attributes["content_type"] = "application/json"
        elif isinstance(value, DataModel):
            data = value.to_json().encode("utf-8")
            attributes["content_type"] = "application/json"
        else:
            raise BadRequestError("Message type not supported")

        if message_id:
            attributes["message_id"] = message_id

        args = {
            "topic": topic_path,
            "data": data,
            "ordering_key": ordering_key,
        } | attributes

        return args

    def convert_pull(
        self,
        subscription_path: str,
        config: MessagePullConfig | None,
    ) -> dict:
        max_messages = 1
        if config and config.max_count is not None:
            max_messages = config.max_count
        args = {
            "subscription": subscription_path,
            "max_messages": max_messages,
        }
        if config and config.max_wait_time is not None:
            args["timeout"] = config.max_wait_time
        if config and config.visibility_timeout is not None:
            args["ack_deadline_seconds"] = config.visibility_timeout
        return args


class ResultConverter:
    def _convert_key(self, message: Any) -> MessageKey:
        return MessageKey(nref=message.ack_id)

    def _convert_value(self, message: Any) -> MessageValueType:
        content_type = message.message.attributes.get(
            "content_type", "text/plain"
        )

        if content_type == "application/octet-stream":
            return message.message.data
        elif content_type == "application/json":
            return json.loads(message.message.data.decode("utf-8"))

        return message.message.data.decode("utf-8")

    def _convert_metadata(self, message: Any) -> dict | None:
        if not message.message.attributes:
            return None

        metadata = dict(message.message.attributes)

        metadata.pop("content_type", None)
        metadata.pop("message_id", None)

        return metadata if metadata else None

    def _convert_properties(self, message: Any) -> MessageProperties:
        attributes = message.message.attributes

        message_id = attributes.get("message_id", None)
        content_type = attributes.get("content_type", "text/plain")

        enqueued_time = None
        if (
            hasattr(message.message, "publish_time")
            and message.message.publish_time
        ):
            enqueued_time = message.message.publish_time.timestamp()

        return MessageProperties(
            message_id=message_id,
            content_type=content_type,
            group_id=(
                message.message.ordering_key
                if hasattr(message.message, "ordering_key")
                else None
            ),
            enqueued_time=enqueued_time,
            delivery_count=(
                message.delivery_attempt
                if hasattr(message, "delivery_attempt")
                else None
            ),
        )

    def _convert_message(
        self,
        message: Any,
    ) -> MessageItem:
        return MessageItem(
            key=self._convert_key(message=message),
            value=self._convert_value(message=message),
            metadata=self._convert_metadata(message=message),
            properties=self._convert_properties(message=message),
        )

    def convert_list_topics(self, nresult: Any) -> list[str]:
        topics = []
        for topic in nresult:
            topics.append(topic.name.split("/")[-1])
        return topics

    def convert_list_subscriptions(self, nresult: Any) -> list[str]:
        subscriptions = []
        for subscription in nresult:
            subscriptions.append(subscription.split("/")[-1])
        return subscriptions

    def convert_pull(self, nresult: Any) -> list[MessageItem]:
        if nresult is None:
            return []
        result = []
        for message in nresult.received_messages:
            result.append(self._convert_message(message))
        return result

    def convert_put(self, nresult: Any) -> None:
        nresult.result()
        return None
