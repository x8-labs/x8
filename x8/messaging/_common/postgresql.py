from __future__ import annotations

import json
import time
import uuid
from typing import Any

import psycopg
from x8.core import Context, DataModel, NCall, Operation, Provider, Response
from x8.core.exceptions import BadRequestError, ConflictError, NotFoundError
from x8.core.time import Time

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
    QueueConfig,
    QueueInfo,
    SubscriptionConfig,
    SubscriptionInfo,
    TopicConfig,
    TopicInfo,
)
from ._operation import MessagingOperation
from ._operation_parser import MessagingOperationParser

DEFAULT_TTL = 7 * 24 * 60 * 60
DEFAULT_VISIBILITY_TIMEOUT = 30


class PostgreSQLBase(Provider):
    mode: MessagingMode
    connection_string: str

    queue: str | None
    topic: str | None
    subscription: str | None

    message_table: str
    metadata_table: str
    poll_interval: float
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _client_helper: Any
    _aclient_helper: Any
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _topic_config_cache: dict[str, TopicConfig] = {}
    _subscription_config_cache: dict[
        str, dict[str, SubscriptionConfig | None]
    ] = {}

    def __init__(
        self,
        mode: MessagingMode,
        connection_string: str,
        queue: str | None = None,
        topic: str | None = None,
        subscription: str | None = None,
        message_table: str = "message",
        metadata_table: str = "metadata",
        poll_interval: float = 0.5,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            mode:
                Messaging mode.
            queue:
                Queue name.
            topic:
                Topic name.
            subscription:
                Subscription name.
            connection_string:
                Connection string to PostgreSQL database.
            message_table:
                PostgreSQL table name for messages.
            metadata_table:
                PostgreSQL table name for metadata.
            poll_interval:
                Poll interval in seconds. Defaults to 0.5.
            nparams:
                Native parameters to PostgreSQL client.
        """
        self.mode = mode
        self.queue = queue
        self.topic = topic
        self.subscription = subscription
        self.connection_string = connection_string
        self.message_table = message_table
        self.metadata_table = metadata_table
        self.poll_interval = poll_interval
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._op_converter = OperationConverter(
            self.message_table,
            self.metadata_table,
        )
        self._result_converter = ResultConverter()
        self._topic_config_cache = dict()
        self._subscription_config_cache = dict()
        if self.mode == MessagingMode.QUEUE:
            self.topic = self.queue
            self.subscription = DEFAULT_SUBSCRIPTION_NAME
        super().__init__(**kwargs)

    def __setup__(
        self,
        context: Context | None = None,
    ) -> None:
        if self._client is not None:
            return

        self._client = psycopg.connect(self.connection_string, **self.nparams)
        self._client_helper = ClientHelper(
            self._client,
            self._op_converter,
            self._result_converter,
            self.mode,
            self.poll_interval,
            self._subscription_config_cache,
        )
        self._create_tables_if_needed()

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return
        self._aclient = await psycopg.AsyncConnection.connect(
            self.connection_string, **self.nparams
        )
        self._aclient_helper = AsyncClientHelper(
            self._aclient,
            self._op_converter,
            self._result_converter,
            self.mode,
            self.poll_interval,
            self._subscription_config_cache,
        )
        await self._acreate_tables_if_needed()

    def _create_tables_if_needed(self, **kwargs: Any) -> None:
        cursor = self._client.cursor()
        rows = cursor.execute(
            """SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = %s""",
            (self.message_table,),
        ).fetchall()
        if len(rows) == 0:
            cursor.execute(
                f"""
                CREATE TABLE {self.message_table} (
                    id TEXT,
                    topic TEXT,
                    subscription TEXT,
                    value BYTEA,
                    metadata TEXT,
                    message_id TEXT,
                    group_id TEXT,
                    content_type TEXT,
                    enqueued_time DOUBLE PRECISION,
                    delivery_count INTEGER,
                    lock_until_time DOUBLE PRECISION,
                    lock_token TEXT,
                    PRIMARY KEY (id, topic, subscription)
                )
                """
            )
        rows = cursor.execute(
            """SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = %s""",
            (self.metadata_table,),
        ).fetchall()
        if len(rows) == 0:
            cursor.execute(
                f"""
                CREATE TABLE {self.metadata_table} (
                    topic TEXT,
                    subscription TEXT,
                    config TEXT,
                    PRIMARY KEY (topic, subscription)
                )
                """
            )
        self._client.commit()
        cursor.close()

    async def _acreate_tables_if_needed(self, **kwargs: Any) -> None:
        cursor = self._aclient.cursor()
        await cursor.execute(
            """SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = %s""",
            (self.message_table,),
        )
        rows = await cursor.fetchall()
        if len(rows) == 0:
            cursor.execute(
                f"""
                CREATE TABLE {self.message_table} (
                    id TEXT,
                    topic TEXT,
                    subscription TEXT,
                    value BYTEA,
                    metadata TEXT,
                    message_id TEXT,
                    group_id TEXT,
                    content_type TEXT,
                    enqueued_time DOUBLE PRECISION,
                    delivery_count INTEGER,
                    lock_until_time DOUBLE PRECISION,
                    lock_token TEXT,
                    PRIMARY KEY (id, topic, subscription)
                )
                """
            )
        await cursor.execute(
            """SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = %s""",
            (self.metadata_table,),
        )
        rows = await cursor.fetchall()
        if len(rows) == 0:
            cursor.execute(
                f"""
                CREATE TABLE {self.metadata_table} (
                    topic TEXT,
                    subscription TEXT,
                    config TEXT,
                    PRIMARY KEY (topic, subscription)
                )
                """
            )
        await self._aclient.commit()
        await cursor.close()

    def _get_topic_name(self, op_parser: MessagingOperationParser) -> str:
        if self.mode == MessagingMode.PUBSUB:
            topic_name = op_parser.get_topic()
            topic_name = topic_name or self.topic or self.__component__.topic
            if topic_name is None:
                raise BadRequestError(
                    "Topic name is required for this operation.",
                )
        else:
            topic_name = op_parser.get_queue()
            topic_name = topic_name or self.queue or self.__component__.queue
            if topic_name is None:
                raise BadRequestError(
                    "Queue name is required for this operation.",
                )
        return topic_name

    def _get_subscription_name(
        self, op_parser: MessagingOperationParser
    ) -> str:
        subscription_name = op_parser.get_subscription()
        subscription_name = (
            subscription_name
            or self.subscription
            or self.__component__.subscription
        )
        if subscription_name is None:
            raise BadRequestError(
                "Subscription name is required for this operation.",
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
        ncall, state = self._get_ncall(
            op_parser,
            self._client_helper,
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(
            nresult,
            state,
            op_parser,
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = MessagingOperationParser(operation)
        ncall, state = self._get_ncall(
            op_parser,
            self._aclient_helper,
        )
        if ncall is None:
            return super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(
            nresult,
            state,
            op_parser,
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: MessagingOperationParser,
        helper: ClientHelper | AsyncClientHelper,
    ) -> tuple[NCall | None, dict]:
        call = None
        state: dict = {}
        op_converter = self._op_converter
        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args = op_converter.convert_create_queue(
                self._get_topic_name(op_parser),
                op_parser.get_queue_config(),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.transact,
                args,
                None,
                {psycopg.errors.UniqueViolation: ConflictError},
            )
        # UPDATE QUEUE
        elif op_parser.op_equals(MessagingOperation.UPDATE_QUEUE):
            args = op_converter.convert_update_queue(
                self._get_topic_name(op_parser),
                op_parser.get_queue_config(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            args = op_converter.convert_drop_topic(
                self._get_topic_name(op_parser),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            args = op_converter.convert_has_topic(
                self._get_topic_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            args = {
                "queue": self._get_topic_name(op_parser),
            }
            call = NCall(
                helper.get_queue,
                args,
            )
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            args = op_converter.convert_list_topics()
            call = NCall(
                helper.execute,
                args,
            )
        # CREATE TOPIC
        elif op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            args = op_converter.convert_create_topic(
                self._get_topic_name(op_parser),
                op_parser.get_topic_config(),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
                {psycopg.errors.UniqueViolation: ConflictError},
            )
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            args = op_converter.convert_update_topic(
                self._get_topic_name(op_parser),
                op_parser.get_topic_config(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            args = op_converter.convert_drop_topic(
                self._get_topic_name(op_parser),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            args = op_converter.convert_has_topic(
                self._get_topic_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            args = {
                "topic": self._get_topic_name(op_parser),
            }
            call = NCall(
                helper.get_topic,
                args,
            )
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            args = op_converter.convert_list_topics()
            call = NCall(
                helper.execute,
                args,
            )
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            args = op_converter.convert_create_subscription(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_subscription_config(),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
                {psycopg.errors.UniqueViolation: ConflictError},
            )
        # UPDATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.UPDATE_SUBSCRIPTION):
            args = op_converter.convert_update_subscription(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_subscription_config(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            args = op_converter.convert_drop_subscription(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            args = op_converter.convert_has_subscription(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
            }
            call = NCall(
                helper.get_subscription,
                args,
            )
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            args = op_converter.convert_list_subscriptions(
                self._get_topic_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = {
                "topic": self._get_topic_name(op_parser),
                "value": op_parser.get_value(),
                "metadata": op_parser.get_metadata(),
                "properties": op_parser.get_properties(),
                "config": op_parser.get_put_config(),
            }
            call = NCall(
                helper.put,
                args,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "topic": self._get_topic_name(op_parser),
                "batch": op_parser.get_batch(),
            }
            call = NCall(
                helper.batch,
                args,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "config": op_parser.get_pull_config(),
            }
            call = NCall(
                helper.pull,
                args,
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = op_converter.convert_ack(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_key(),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = op_converter.convert_nack(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_key(),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "key": op_parser.get_key(),
                "timeout": op_parser.get_timeout(),
            }
            call = NCall(
                helper.extend,
                args,
            )
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            args = op_converter.convert_purge(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {
                "topic": op_parser.get_topic() or op_parser.get_queue(),
            }
            call = NCall(helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict,
        op_parser: MessagingOperationParser,
    ) -> Any:
        result: Any = None
        result_converter = self._result_converter
        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            result = None
        # UPDATE QUEUE
        elif op_parser.op_equals(MessagingOperation.UPDATE_QUEUE):
            if nresult == 0:
                raise NotFoundError(
                    "Queue not found.",
                )
            result = None
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            where_exists = op_parser.get_where_exists()
            if nresult == 0 and where_exists is True:
                raise NotFoundError
            result = None
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            return len(nresult) > 0
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
            if nresult == 0:
                raise NotFoundError(
                    "Topic not found.",
                )
            result = None
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            where_exists = op_parser.get_where_exists()
            if nresult == 0 and where_exists is True:
                raise NotFoundError
            result = None
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            return len(nresult) > 0
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
            if nresult == 0:
                raise NotFoundError(
                    "Subscription not found.",
                )
            result = None
        # DROP SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.DROP_SUBSCRIPTION):
            where_exists = op_parser.get_where_exists()
            if nresult == 0 and where_exists is True:
                raise NotFoundError
            result = None
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            result = result_converter.convert_list_subscriptions(nresult)
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            return len(nresult) > 0
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
            result = nresult
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            if nresult == 0:
                raise BadRequestError(
                    "Message not found or already acknowledged.",
                )
            result = None
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            if nresult == 0:
                raise BadRequestError(
                    "Message not found or already acknowledged.",
                )
            result = None
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            if nresult == 0:
                raise BadRequestError(
                    "Message not found or already acknowledged.",
                )
            result = MessageItem(key=op_parser.get_key())
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            result = None
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            pass
        return result


class ClientHelper:
    client: Any
    op_converter: OperationConverter
    result_converter: ResultConverter
    mode: MessagingMode
    poll_interval: float
    subscription_config_cache: dict[str, dict[str, SubscriptionConfig | None]]

    def __init__(
        self,
        client: Any,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
        mode: MessagingMode,
        poll_interval: float,
        subscription_config_cache: dict[
            str, dict[str, SubscriptionConfig | None]
        ],
    ):
        self.client = client
        self.op_converter = op_converter
        self.result_converter = result_converter
        self.mode = mode
        self.poll_interval = poll_interval
        self.subscription_config_cache = subscription_config_cache

    def _get_subscription_config(
        self,
        topic_name: str,
        subscription_name: str,
    ) -> SubscriptionConfig | None:
        if (
            topic_name in self.subscription_config_cache
            and subscription_name in self.subscription_config_cache[topic_name]
        ):
            return self.subscription_config_cache[topic_name][
                subscription_name
            ]
        args = self.op_converter.convert_get_subscription_config(
            topic_name,
            subscription_name,
        )
        nresult = self.execute(
            args["query"],
            args["params"],
            fetchall=True,
        )
        if nresult is None or len(nresult) == 0:
            raise BadRequestError(
                f"Subscription {subscription_name} not found.",
            )
        if nresult[0][0] is None:
            config = None
        else:
            config = SubscriptionConfig.from_dict(json.loads(nresult[0][0]))
        if topic_name not in self.subscription_config_cache:
            self.subscription_config_cache[topic_name] = {}
        self.subscription_config_cache[topic_name][subscription_name] = config
        return config

    def get_queue(self, queue: str) -> QueueInfo:
        sinfo = self.get_subscription(queue, DEFAULT_SUBSCRIPTION_NAME)
        print(sinfo)
        return QueueInfo(
            name=queue,
            active_message_count=sinfo.active_message_count,
            inflight_message_count=sinfo.inflight_message_count,
            scheduled_message_count=sinfo.scheduled_message_count,
            config=(
                QueueConfig.from_dict(sinfo.config.to_dict())
                if sinfo.config
                else None
            ),
            nref=queue,
        )

    def get_topic(self, topic: str) -> TopicInfo:
        args = self.op_converter.convert_get_topic_config(topic)
        nresult = self.execute(args["query"], args["params"])
        if nresult is None:
            raise NotFoundError
        config_json = nresult[0]
        args = self.op_converter.convert_get_subscription_count(topic)
        subscription_count = self.execute(
            args["query"], args["params"], fetchall=True
        )[0][0]
        return TopicInfo(
            name=topic,
            subscription_count=subscription_count - 1,
            config=(
                TopicConfig.from_dict(json.loads(config_json))
                if config_json
                else None
            ),
            nref=topic,
        )

    def get_subscription(
        self, topic: str, subscription: str
    ) -> SubscriptionInfo:
        args = self.op_converter.convert_get_subscription_config(
            topic, subscription
        )
        nresult = self.execute(args["query"], args["params"])
        if nresult is None:
            raise NotFoundError
        config_json = nresult[0]
        args = self.op_converter.convert_get_active_message_count(
            topic, subscription
        )
        active_message_count = self.execute(
            args["query"], args["params"], fetchall=True
        )[0][0]
        args = self.op_converter.convert_get_inflight_message_count(
            topic, subscription
        )
        inflight_message_count = self.execute(
            args["query"], args["params"], fetchall=True
        )[0][0]
        args = self.op_converter.convert_get_scheduled_message_count(
            topic, subscription
        )
        scheduled_message_count = self.execute(
            args["query"], args["params"], fetchall=True
        )[0][0]
        return SubscriptionInfo(
            name=subscription,
            topic=topic,
            active_message_count=active_message_count,
            inflight_message_count=inflight_message_count,
            scheduled_message_count=scheduled_message_count,
            config=(
                SubscriptionConfig.from_dict(json.loads(config_json))
                if config_json
                else None
            ),
            nref=subscription,
        )

    def put(
        self,
        topic: str,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: MessageProperties | None = None,
        config: MessagePutConfig | None = None,
    ) -> None:
        args = self.op_converter.convert_list_subscriptions(topic)
        nresult = self.execute(
            query=args["query"], params=args["params"], fetchall=True
        )
        subscriptions = self.result_converter.convert_list_subscriptions(
            nresult
        )
        if len(subscriptions) == 0:
            if self.mode == MessagingMode.QUEUE:
                raise BadRequestError(
                    f"Queue {topic} not found.",
                )
        if properties and properties.message_id:
            id = properties.message_id
        else:
            id = str(uuid.uuid4())
        enqueue_time = Time.now()
        if config and config.delay:
            enqueue_time += config.delay
        ops: list = []
        for subscription in subscriptions:
            args = self.op_converter.convert_put(
                id,
                topic,
                subscription,
                enqueue_time,
                value,
                metadata,
                properties,
            )
            ops.append(args)
        self.transact(ops=ops)

    def batch(
        self,
        topic: str,
        batch: MessageBatch,
    ) -> None:
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            self.put(
                topic=topic,
                value=op_parser.get_value(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                config=op_parser.get_put_config(),
            )

    def pull(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None = None,
    ) -> list[MessageItem]:
        subscription_config = self._get_subscription_config(
            topic,
            subscription,
        )
        result: list[MessageItem] = []
        start_time = Time.now()
        while True:
            now = Time.now()
            found = False
            with self.client.transaction():
                args = self.op_converter.convert_pull(
                    topic,
                    subscription,
                )
                nresult = self.client.execute(
                    args["query"], args["params"]
                ).fetchone()
                if nresult is not None:
                    lock_token = str(uuid.uuid4())
                    message = self.result_converter.convert_pull(nresult)
                    if message.key is None or message.key.id is None:
                        raise BadRequestError(
                            "Message key is missing.",
                        )
                    lock_duration = (
                        config.visibility_timeout if config else None
                    )
                    lock_duration = lock_duration or (
                        subscription_config.visibility_timeout
                        if subscription_config
                        else DEFAULT_VISIBILITY_TIMEOUT
                    )
                    lock_until_time = now + lock_duration
                    args = self.op_converter.convert_lock(
                        message.key.id,
                        topic,
                        subscription,
                        lock_until_time,
                        lock_token,
                        message.key.nref,
                    )
                    updated = self.client.execute(
                        args["query"], args["params"]
                    ).rowcount
                    if updated == 1:
                        message.key.nref = lock_token
                        result.append(message)
                        found = True
            if config and config.max_wait_time:
                if now - start_time > config.max_wait_time:
                    break
            if config and config.max_count:
                if len(result) >= config.max_count:
                    break
            if not config or not config.max_count:
                if len(result) > 0:
                    break
            if not found:
                time.sleep(self.poll_interval)
        return result

    def extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None = None,
    ) -> None:
        subscription_config = self._get_subscription_config(
            topic, subscription
        )
        args = self.op_converter.convert_extend(
            topic,
            subscription,
            key,
            timeout,
            subscription_config,
        )
        self.client.execute(args["query"], args["params"])

    def execute(
        self,
        query: str,
        params: tuple | None = None,
        rowcount: bool = False,
        fetchall: bool = False,
    ) -> Any:
        cursor = self.client.cursor()
        try:
            cursor.execute(query, params)
            if rowcount:
                return cursor.rowcount
            if fetchall:
                return cursor.fetchall()
            if cursor.rowcount > 0:
                return cursor.fetchone()
        finally:
            self.client.commit()
            cursor.close()

    def transact(self, ops: list) -> Any:
        result = []
        cursor = self.client.cursor()
        try:
            for op in ops:
                if "params" in op:
                    cursor.execute(op["query"], op["params"])
                else:
                    cursor.execute(op["query"])
                if cursor.rowcount == 0:
                    raise ConflictError
                if "rowcount" in op and op["rowcount"]:
                    result.append(cursor.rowcount)
                elif cursor.rowcount > 0:
                    result.append(cursor.fetchone())
            self.client.commit()
        except Exception:
            self.client.rollback()
            raise
        finally:
            cursor.close()
        return result

    def close(self, topic: str | None) -> Any:
        if topic is not None:
            return
        self.client.close()


class AsyncClientHelper:
    client: Any
    op_converter: OperationConverter
    result_converter: ResultConverter
    mode: MessagingMode
    poll_interval: float
    subscription_config_cache: dict[str, dict[str, SubscriptionConfig | None]]

    def __init__(
        self,
        client: Any,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
        mode: MessagingMode,
        poll_interval: float,
        subscription_config_cache: dict[
            str, dict[str, SubscriptionConfig | None]
        ],
    ):
        self.client = client
        self.op_converter = op_converter
        self.result_converter = result_converter
        self.mode = mode
        self.poll_interval = poll_interval
        self.subscription_config_cache = subscription_config_cache

    async def _get_subscription_config(
        self,
        topic_name: str,
        subscription_name: str,
    ) -> SubscriptionConfig | None:
        if (
            topic_name in self.subscription_config_cache
            and subscription_name in self.subscription_config_cache[topic_name]
        ):
            return self.subscription_config_cache[topic_name][
                subscription_name
            ]
        args = self.op_converter.convert_get_subscription_config(
            topic_name,
            subscription_name,
        )
        nresult = await self.execute(
            args["query"],
            args["params"],
            fetchall=True,
        )
        if nresult is None or len(nresult) == 0:
            raise BadRequestError(
                f"Subscription {subscription_name} not found.",
            )
        if nresult[0][0] is None:
            config = None
        else:
            config = SubscriptionConfig.from_dict(json.loads(nresult[0][0]))
        if topic_name not in self.subscription_config_cache:
            self.subscription_config_cache[topic_name] = {}
        self.subscription_config_cache[topic_name][subscription_name] = config
        return config

    async def get_queue(self, queue: str) -> QueueInfo:
        sinfo = await self.get_subscription(queue, DEFAULT_SUBSCRIPTION_NAME)
        return QueueInfo(
            name=queue,
            active_message_count=sinfo.active_message_count,
            inflight_message_count=sinfo.inflight_message_count,
            scheduled_message_count=sinfo.scheduled_message_count,
            config=(
                QueueConfig.from_dict(sinfo.config.to_dict())
                if sinfo.config
                else None
            ),
            nref=queue,
        )

    async def get_topic(self, topic: str) -> TopicInfo:
        args = self.op_converter.convert_get_topic_config(topic)
        nresult = await self.execute(args["query"], args["params"])
        if nresult is None:
            raise NotFoundError
        config_json = nresult[0]
        args = self.op_converter.convert_get_subscription_count(topic)
        subscription_count = (
            await self.execute(args["query"], args["params"], fetchall=True)
        )[0][0]
        return TopicInfo(
            name=topic,
            subscription_count=subscription_count - 1,
            config=(
                TopicConfig.from_dict(json.loads(config_json))
                if config_json
                else None
            ),
            nref=topic,
        )

    async def get_subscription(
        self, topic: str, subscription: str
    ) -> SubscriptionInfo:
        args = self.op_converter.convert_get_subscription_config(
            topic, subscription
        )
        nresult = await self.execute(args["query"], args["params"])
        if nresult is None:
            raise NotFoundError
        config_json = nresult[0]
        args = self.op_converter.convert_get_active_message_count(
            topic, subscription
        )
        active_message_count = (
            await self.execute(args["query"], args["params"], fetchall=True)
        )[0][0]
        args = self.op_converter.convert_get_inflight_message_count(
            topic, subscription
        )
        inflight_message_count = (
            await self.execute(args["query"], args["params"], fetchall=True)
        )[0][0]
        args = self.op_converter.convert_get_scheduled_message_count(
            topic, subscription
        )
        scheduled_message_count = (
            await self.execute(args["query"], args["params"], fetchall=True)
        )[0][0]
        return SubscriptionInfo(
            name=subscription,
            topic=topic,
            active_message_count=active_message_count,
            inflight_message_count=inflight_message_count,
            scheduled_message_count=scheduled_message_count,
            config=(
                SubscriptionConfig.from_dict(json.loads(config_json))
                if config_json
                else None
            ),
            nref=subscription,
        )

    async def put(
        self,
        topic: str,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: MessageProperties | None = None,
        config: MessagePutConfig | None = None,
    ) -> None:
        args = self.op_converter.convert_list_subscriptions(topic)
        nresult = await self.execute(
            query=args["query"], params=args["params"], fetchall=True
        )
        subscriptions = self.result_converter.convert_list_subscriptions(
            nresult
        )
        if len(subscriptions) == 0:
            if self.mode == MessagingMode.QUEUE:
                raise BadRequestError(
                    f"Queue {topic} not found.",
                )
        if properties and properties.message_id:
            id = properties.message_id
        else:
            id = str(uuid.uuid4())
        enqueue_time = Time.now()
        if config and config.delay:
            enqueue_time += config.delay
        ops: list = []
        for subscription in subscriptions:
            args = self.op_converter.convert_put(
                id,
                topic,
                subscription,
                enqueue_time,
                value,
                metadata,
                properties,
            )
            ops.append(args)
        await self.transact(ops=ops)

    async def batch(
        self,
        topic: str,
        batch: MessageBatch,
    ) -> None:
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            await self.put(
                topic=topic,
                value=op_parser.get_value(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                config=op_parser.get_put_config(),
            )

    async def pull(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None = None,
    ) -> list[MessageItem]:
        subscription_config = await self._get_subscription_config(
            topic,
            subscription,
        )
        result: list[MessageItem] = []
        start_time = Time.now()
        while True:
            now = Time.now()
            found = False
            async with self.client.transaction():
                async with self.client.cursor() as cursor:
                    args = self.op_converter.convert_pull(
                        topic,
                        subscription,
                    )
                    await cursor.execute(args["query"], args["params"])
                    nresult = await cursor.fetchone()
                    if nresult is not None:
                        lock_token = str(uuid.uuid4())
                        message = self.result_converter.convert_pull(nresult)
                        if message.key is None or message.key.id is None:
                            raise BadRequestError(
                                "Message key is missing.",
                            )
                        lock_duration = (
                            config.visibility_timeout if config else None
                        )
                        lock_duration = lock_duration or (
                            subscription_config.visibility_timeout
                            if subscription_config
                            else DEFAULT_VISIBILITY_TIMEOUT
                        )
                        lock_until_time = now + lock_duration
                        args = self.op_converter.convert_lock(
                            message.key.id,
                            topic,
                            subscription,
                            lock_until_time,
                            lock_token,
                            message.key.nref,
                        )
                        await cursor.execute(args["query"], args["params"])
                        updated = cursor.rowcount
                        if updated == 1:
                            message.key.nref = lock_token
                            result.append(message)
                            found = True
            if config and config.max_wait_time:
                if now - start_time > config.max_wait_time:
                    break
            if config and config.max_count:
                if len(result) >= config.max_count:
                    break
            if not config or not config.max_count:
                if len(result) > 0:
                    break
            if not found:
                time.sleep(self.poll_interval)
        return result

    async def extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None = None,
    ) -> None:
        subscription_config = await self._get_subscription_config(
            topic, subscription
        )
        args = self.op_converter.convert_extend(
            topic,
            subscription,
            key,
            timeout,
            subscription_config,
        )
        await self.client.execute(args["query"], args["params"])

    async def execute(
        self,
        query: str,
        params: tuple | None = None,
        rowcount: bool = False,
        fetchall: bool = False,
    ) -> Any:
        cursor = self.client.cursor()
        try:
            await cursor.execute(query, params)
            if rowcount:
                return cursor.rowcount
            if fetchall:
                return await cursor.fetchall()
            if cursor.rowcount > 0:
                return await cursor.fetchone()
        finally:
            await self.client.commit()
            await cursor.close()

    async def transact(self, ops: list) -> Any:
        result = []
        cursor = self.client.cursor()
        try:
            for op in ops:
                if "params" in op:
                    await cursor.execute(op["query"], op["params"])
                else:
                    await cursor.execute(op["query"])
                if cursor.rowcount == 0:
                    raise ConflictError
                if "rowcount" in op and op["rowcount"]:
                    result.append(cursor.rowcount)
                elif cursor.rowcount > 0:
                    result.append(cursor.fetchone())
            await self.client.commit()
        except Exception:
            await self.client.rollback()
            raise
        finally:
            await cursor.close()
        return result

    async def close(self, topic: str | None) -> Any:
        if topic is not None:
            return
        await self.client.close()


class OperationConverter:
    message_table: str
    metadata_table: str

    META_SUBSCRIPTION_NAME = "#"

    def __init__(
        self,
        message_table: str,
        metadata_table: str,
    ):
        self.message_table = message_table
        self.metadata_table = metadata_table

    def convert_get_active_message_count(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        current_time = Time.now()
        query = f"""
            SELECT COUNT(*)
            FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            AND lock_until_time < %s
            AND enqueued_time <= %s
            """
        params = (
            topic,
            subscription,
            current_time,
            current_time,
        )
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_get_inflight_message_count(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        current_time = Time.now()
        query = f"""
            SELECT COUNT(*)
            FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            AND lock_until_time >= %s
            """
        params = (
            topic,
            subscription,
            current_time,
        )
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_get_scheduled_message_count(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        current_time = Time.now()
        query = f"""
            SELECT COUNT(*)
            FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            AND enqueued_time > %s
            """
        params = (
            topic,
            subscription,
            current_time,
        )
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_get_subscription_count(
        self,
        topic: str,
    ) -> dict:
        query = f"""
            SELECT COUNT(*)
            FROM {self.metadata_table}
            WHERE topic = %s
            """
        params = (topic,)
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_get_topic_config(
        self,
        topic: str,
    ) -> dict:
        query = f"""
            SELECT config
            FROM {self.metadata_table}
            WHERE topic = %s AND subscription = %s
            """
        params = (topic, OperationConverter.META_SUBSCRIPTION_NAME)
        return {
            "query": query,
            "params": params,
        }

    def convert_get_subscription_config(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        query = f"""
            SELECT config
            FROM {self.metadata_table}
            WHERE topic = %s AND subscription = %s
            """
        params = (topic, subscription)
        return {
            "query": query,
            "params": params,
        }

    def convert_create_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        where_exists: bool | None,
    ) -> dict:
        ops: list = []
        args = self.convert_create_topic(
            queue,
            config,
            where_exists,
        )
        ops.append(args)
        args = self.convert_create_subscription(
            queue,
            DEFAULT_SUBSCRIPTION_NAME,
            config,
            where_exists,
        )
        ops.append(args)
        return {"ops": ops}

    def convert_update_queue(
        self,
        queue: str,
        config: QueueConfig | None,
    ) -> dict:
        return self.convert_update_subscription(
            queue,
            DEFAULT_SUBSCRIPTION_NAME,
            config,
        )

    def convert_create_topic(
        self,
        topic: str,
        config: QueueConfig | TopicConfig | None,
        where_exists: bool | None,
    ) -> dict:
        if where_exists is False:
            query = f"""
                INSERT INTO {self.metadata_table}
                (topic, subscription, config)
                VALUES (%s, %s, %s)
                """
        else:
            query = f"""
                INSERT INTO {self.metadata_table}
                (topic, subscription, config)
                VALUES (%s, %s, %s)
                ON CONFLICT (topic, subscription)
                DO UPDATE SET
                config = EXCLUDED.config
                """
        params = (
            topic,
            OperationConverter.META_SUBSCRIPTION_NAME,
            config.to_json() if config else None,
        )
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_update_topic(
        self,
        topic: str,
        config: TopicConfig | None,
    ) -> dict:
        query = f"""
            UPDATE {self.metadata_table}
            SET config = %s
            WHERE topic = %s AND subscription = %s
            """
        params = (
            config.to_json() if config else None,
            topic,
            OperationConverter.META_SUBSCRIPTION_NAME,
        )
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_drop_topic(
        self,
        topic: str,
        where_exists: bool | None,
    ) -> dict:
        query = f"""
            DELETE FROM {self.metadata_table}
            WHERE topic = %s
            """
        params = (topic,)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_has_topic(
        self,
        topic: str,
    ) -> dict:
        query = f"""
            SELECT topic
            FROM {self.metadata_table}
            WHERE topic = %s
            """
        params = (topic,)
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_list_topics(self) -> dict:
        query = f"""
            SELECT topic
            FROM {self.metadata_table}
            WHERE subscription = %s
            """
        params = (OperationConverter.META_SUBSCRIPTION_NAME,)
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_create_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | None,
        where_exists: bool | None,
    ) -> dict:
        if where_exists is False:
            query = f"""
                INSERT INTO {self.metadata_table}
                (topic, subscription, config)
                VALUES (%s, %s, %s)
                """
        else:
            query = f"""
                INSERT INTO {self.metadata_table}
                (topic, subscription, config)
                VALUES (%s, %s, %s)
                ON CONFLICT (topic, subscription)
                DO UPDATE SET
                config = EXCLUDED.config
                """
        params = (
            topic,
            subscription,
            config.to_json() if config else None,
        )
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_update_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | QueueConfig | None,
    ) -> dict:
        query = f"""
            UPDATE {self.metadata_table}
            SET config = %s
            WHERE topic = %s AND subscription = %s
            """
        params = (
            config.to_json() if config else None,
            topic,
            subscription,
        )
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_drop_subscription(
        self,
        topic: str,
        subscription: str,
        where_exists: bool | None,
    ) -> dict:
        query = f"""
            DELETE FROM {self.metadata_table}
            WHERE topic = %s AND subscription = %s
            """
        params = (topic, subscription)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_has_subscription(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        query = f"""
            SELECT topic, subscription
            FROM {self.metadata_table}
            WHERE topic = %s AND subscription = %s
            """
        params = (topic, subscription)
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_list_subscriptions(self, topic: str) -> dict:
        query = f"""
            SELECT subscription
            FROM {self.metadata_table}
            WHERE topic = %s
            AND subscription != %s
            """
        params = (
            topic,
            OperationConverter.META_SUBSCRIPTION_NAME,
        )
        return {
            "query": query,
            "params": params,
            "fetchall": True,
        }

    def convert_put(
        self,
        id: str,
        topic: str,
        subscription: str,
        enqueue_time: float,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
    ) -> dict:
        message_id = None
        content_type = None
        group_id = None
        body: bytes | None = None
        if properties is not None:
            message_id = properties.message_id
            content_type = properties.content_type
            group_id = properties.group_id
        if isinstance(value, str):
            body = value.encode("utf-8")
            content_type = "text/plain"
        elif isinstance(value, bytes):
            body = value
            content_type = "application/octet-stream"
        elif isinstance(value, dict):
            body = json.dumps(value).encode("utf-8")
            content_type = "application/json"
        elif isinstance(value, DataModel):
            body = value.to_json().encode("utf-8")
            content_type = "application/json"
        else:
            raise BadRequestError("Message type not supported")
        query = f"""
            INSERT INTO {self.message_table}
            (id, topic, subscription, value, metadata,
            message_id, group_id, content_type,
            enqueued_time, delivery_count,
            lock_until_time, lock_token)
            VALUES (%s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s)
            """
        params = (
            id,
            topic,
            subscription,
            body,
            json.dumps(metadata) if metadata else None,
            message_id,
            group_id,
            content_type,
            enqueue_time,
            0,
            0,
            "",
        )
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_pull(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        current_time = Time.now()
        query = f"""
            SELECT id, topic, subscription, value, metadata,
            message_id, group_id, content_type,
            enqueued_time, delivery_count,
            lock_until_time, lock_token
            FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            AND lock_until_time <= %s
            AND enqueued_time <= %s
            ORDER BY enqueued_time
            LIMIT 1
            """
        params = (
            topic,
            subscription,
            current_time,
            current_time,
        )
        return {
            "query": query,
            "params": params,
        }

    def convert_lock(
        self,
        id: str,
        topic: str,
        subscription: str,
        lock_until_time: float,
        lock_token: str,
        previous_lock_token: str | None,
    ) -> dict:
        query = f"""
            UPDATE {self.message_table}
            SET lock_until_time = %s, lock_token = %s,
            delivery_count = delivery_count + 1
            WHERE topic = %s AND subscription = %s
            AND id = %s AND lock_token = %s
            """
        params = (
            lock_until_time,
            lock_token,
            topic,
            subscription,
            id,
            previous_lock_token,
        )
        return {
            "query": query,
            "params": params,
        }

    def convert_ack(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
    ) -> dict:
        query = f"""
            DELETE FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            AND lock_token = %s
            """
        params = (topic, subscription, key.nref)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_nack(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
    ) -> dict:
        query = f"""
            UPDATE {self.message_table}
            SET lock_until_time = %s, lock_token = %s
            WHERE topic = %s AND subscription = %s
            AND lock_token = %s
            """
        params = (0, "", topic, subscription, key.nref)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None,
        subscription_config: SubscriptionConfig | None = None,
    ) -> dict:
        lock_duration = (
            subscription_config.visibility_timeout
            if subscription_config and subscription_config.visibility_timeout
            else DEFAULT_VISIBILITY_TIMEOUT
        )
        t = timeout or lock_duration
        t = Time.now() + t
        query = f"""
            UPDATE {self.message_table}
            SET lock_until_time = %s
            WHERE topic = %s AND subscription = %s
            AND lock_token = %s
            """
        params = (t, topic, subscription, key.nref)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }

    def convert_purge(
        self,
        topic: str,
        subscription: str,
    ) -> dict:
        query = f"""
            DELETE FROM {self.message_table}
            WHERE topic = %s AND subscription = %s
            """
        params = (topic, subscription)
        return {
            "query": query,
            "params": params,
            "rowcount": True,
        }


class ResultConverter:
    def __init__(self):
        pass

    def _convert_value(
        self,
        value: bytes,
        content_type: str | None,
    ) -> MessageValueType:
        if content_type == "text/plain":
            return value.decode("utf-8")
        elif content_type == "application/octet-stream":
            return value
        elif content_type == "application/json":
            return json.loads(value.decode("utf-8"))
        return value

    def convert_list_topics(self, nresult: Any) -> list[str]:
        topics: list[str] = []
        for row in nresult:
            topics.append(row[0])
        return topics

    def convert_list_subscriptions(self, nresult: Any) -> list[str]:
        subscriptions: list[str] = []
        for row in nresult:
            subscriptions.append(row[0])
        return subscriptions

    def convert_pull(
        self,
        nresult: Any,
    ) -> MessageItem:
        id = nresult[0]
        value = nresult[3]
        metadata = nresult[4]
        message_id = nresult[5]
        group_id = nresult[6]
        content_type = nresult[7]
        enqueued_time = nresult[8]
        delivery_count = nresult[9]
        lock_token = nresult[11]

        message = MessageItem(
            key=MessageKey(id=id, nref=lock_token),
            value=self._convert_value(
                value,
                content_type,
            ),
            metadata=json.loads(metadata) if metadata else None,
            properties=MessageProperties(
                message_id=message_id,
                group_id=group_id,
                content_type=content_type,
                enqueued_time=enqueued_time,
                delivery_count=delivery_count + 1,
            ),
        )
        return message
