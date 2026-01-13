from __future__ import annotations

import json
import time
import uuid
from typing import Any

import redis
import redis.asyncio as aredis

from x8._common.redis_provider import RedisProvider
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


class RedisBase(RedisProvider):
    mode: MessagingMode

    queue: str | None
    topic: str | None
    subscription: str | None

    visibility_timeout: int
    check_pending_interval: float
    nparams: dict[str, Any]

    _client: redis.Redis | None
    _aclient: aredis.Redis | None
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _worker_id: str
    _last_pending_check: float

    def __init__(
        self,
        mode: MessagingMode = MessagingMode.QUEUE,
        queue: str | None = None,
        topic: str | None = None,
        subscription: str | None = None,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        options: dict | None = None,
        visibility_timeout: int = 30,
        check_pending_interval: float = 30,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            mode:
                Messaging mode. Defaults to QUEUE.
            queue:
                Queue name.
            topic:
                Topic name.
            subscription:
                Subscription name.
            url:
                Redis URL.
            host:
                Redis host.
            port:
                Redis port.
            db:
                Redis database.
            username:
                Redis username.
            password:
                Redis password.
            options:
                Redis options.
            visibility_timeout:
                Visibility timeout in seconds. Defaults to 30.
            check_pending_interval:
                Interval to check the pending queue.
                Defaults to 30.
            nparams:
                Native parameters to Redis client.
        """

        self.mode = mode
        self.queue = queue
        self.topic = topic
        self.subscription = subscription
        self.visibility_timeout = visibility_timeout
        self.check_pending_interval = check_pending_interval
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        self._worker_id = str(uuid.uuid4())
        self._last_pending_check = 0

        if self.mode == MessagingMode.QUEUE:
            self.topic = self.queue
            self.subscription = self.queue
        RedisProvider.__init__(
            self,
            url=url,
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            options=options,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        self._client, _ = self._get_client_and_lib(decode_responses=False)

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        self._aclient, self._alib = self._aget_client_and_lib(
            decode_responses=False
        )

    def _get_topic_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        if self.mode == MessagingMode.PUBSUB:
            name = (
                op_parser.get_topic() or self.topic or self.__component__.topic
            )
        else:
            name = (
                op_parser.get_queue() or self.queue or self.__component__.queue
            )
        return name

    def _get_subscription_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        name = (
            op_parser.get_subscription()
            or self.subscription
            or self.__component__.subscription
        )
        if name is None and self.mode == MessagingMode.QUEUE:
            name = self._get_topic_name(op_parser)
        return name

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
            self._client,
            ClientHelper(
                self._client,
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

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = MessagingOperationParser(operation)
        ncall = self._get_ncall(
            op_parser,
            self._aclient,
            AsyncClientHelper(
                self._aclient,
                self._op_converter,
            ),
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
        client: Any,
        client_helper: ClientHelper | AsyncClientHelper,
    ) -> NCall | None:
        call = None
        op_converter = self._op_converter
        nargs = op_parser.get_nargs()

        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args: dict[str, Any] | list = {
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
                "topic": self._get_topic_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.drop_topic,
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
            args = {
                "nargs": nargs,
            }
            call = NCall(
                client_helper.list_topics,
                args,
                None,
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
            call = NCall(
                client_helper.drop_topic,
                args,
                None,
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
            args = {
                "nargs": nargs,
            }
            call = NCall(
                client_helper.list_topics,
                args,
                None,
            )
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
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
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
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
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.drop_subscription,
                args,
                None,
            )
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
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
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
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
                "topic": self._get_topic_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.list_subscriptions,
                args,
                None,
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = op_converter.convert_put(
                self._get_topic_name(op_parser),
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                client.xadd,
                args,
                None,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "topic": self._get_topic_name(op_parser),
                "batch": op_parser.get_batch(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.batch,
                args,
                None,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            current_time = time.time()
            check_pending = (
                current_time - self._last_pending_check
                > self.check_pending_interval
            )
            if check_pending:
                self._last_pending_check = current_time
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "config": op_parser.get_pull_config(),
                "worker_id": self._worker_id,
                "visible_timeout": self.visibility_timeout,
                "check_pending": check_pending,
                "nargs": nargs,
            }
            call = NCall(
                client_helper.pull,
                args,
                None,
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = op_converter.convert_ack(
                self._get_topic_name(op_parser),
                self._get_subscription_name(op_parser),
                op_parser.get_key(),
            )
            call = NCall(
                client.xack,
                args,
                None,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "key": op_parser.get_key(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.nack,
                args,
                None,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "key": op_parser.get_key(),
                "timeout": op_parser.get_timeout(),
                "worker_id": self._worker_id,
                "nargs": nargs,
            }
            call = NCall(
                client_helper.extend,
                args,
                None,
            )
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            args = {
                "topic": self._get_topic_name(op_parser),
                "subscription": self._get_subscription_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.purge,
                args,
                None,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {
                "topic": op_parser.get_topic() or op_parser.get_queue(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.close,
                args,
                None,
            )
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
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            result = nresult
        # GET TOPIC
        elif op_parser.op_equals(MessagingOperation.GET_TOPIC):
            result = nresult
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
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
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            result = nresult
        # GET SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.GET_SUBSCRIPTION):
            result = nresult
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
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


class ClientHelper:
    client: Any
    op_converter: OperationConverter

    def __init__(
        self,
        client: Any,
        op_converter: OperationConverter,
    ):
        self.client = client
        self.op_converter = op_converter

    def create_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        try:
            self.client.xgroup_create(
                name=queue, groupname=queue, id="$", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    def update_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        nargs: Any,
    ):
        if not self.client.exists(queue):
            raise NotFoundError

    def has_queue(
        self,
        queue: str,
        nargs: Any,
    ) -> bool:
        return self.client.exists(queue) > 0

    def get_queue(
        self,
        queue: str,
        nargs: Any,
    ) -> QueueInfo:
        if not self.client.exists(queue):
            raise NotFoundError
        groups = self.client.xinfo_groups(name=queue)
        group = next(
            (
                group
                for group in groups
                if group["name"].decode("utf-8") == queue
            ),
            None,
        )
        if not group:
            raise NotFoundError
        active_count = self.count(
            queue, group["last-delivered-id"].decode("utf-8")
        )
        return QueueInfo(
            name=queue,
            active_message_count=active_count,
            inflight_message_count=group["pending"] if group else None,
            config=QueueConfig(),
            nref=queue,
        )

    def create_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        if self.client.exists(topic):
            if where_exists is False:
                raise ConflictError
            return
        try:
            self.client.xgroup_create(
                name=topic, groupname="dummy", id="$", mkstream=True
            )
            self.client.xgroup_destroy(
                name=topic,
                groupname="dummy",
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    def update_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        nargs: Any,
    ):
        if not self.client.exists(topic):
            raise NotFoundError

    def drop_topic(
        self,
        topic: str,
        where_exists: bool | None,
        nargs: Any,
    ):
        deleted = self.client.delete(topic)
        if not deleted and where_exists is True:
            raise NotFoundError

    def has_topic(
        self,
        topic: str,
        nargs: Any,
    ) -> bool:
        return self.client.exists(topic) > 0

    def get_topic(
        self,
        topic: str,
        nargs: Any,
    ) -> TopicInfo:
        if not self.client.exists(topic):
            raise NotFoundError
        groups = self.client.xinfo_groups(name=topic)
        return TopicInfo(
            name=topic,
            subscription_count=len(groups),
            config=TopicConfig(),
            nref=topic,
        )

    def list_topics(self, nargs: Any):
        result = []
        cursor = 0
        while True:
            cursor, keys = self.client.scan(cursor=cursor, _type="stream")
            for key in keys:
                if key.startswith(b"__"):
                    continue
                result.append(key.decode("utf-8"))
            if cursor == 0:
                break
        return result

    def create_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        try:
            self.client.xgroup_create(
                name=topic, groupname=subscription, id="$", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    def update_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ):
        if not self.client.exists(topic):
            raise NotFoundError
        groups = self.client.xinfo_groups(name=topic)
        if not any(
            group["name"].decode("utf-8") == subscription for group in groups
        ):
            raise NotFoundError

    def drop_subscription(
        self,
        topic: str,
        subscription: str,
        where_exists: bool | None,
        nargs: Any,
    ):
        deleted = self.client.xgroup_destroy(
            name=topic,
            groupname=subscription,
        )
        if not deleted and where_exists is True:
            raise NotFoundError

    def has_subscription(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ) -> bool:
        groups = self.client.xinfo_groups(name=topic)
        return any(
            group["name"].decode("utf-8") == subscription for group in groups
        )

    def get_subscription(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        if not self.client.exists(topic):
            raise NotFoundError
        groups = self.client.xinfo_groups(name=topic)
        group = next(
            (
                group
                for group in groups
                if group["name"].decode("utf-8") == subscription
            ),
            None,
        )
        if not group:
            raise NotFoundError
        active_count = self.count(
            topic, group["last-delivered-id"].decode("utf-8")
        )
        return SubscriptionInfo(
            name=subscription,
            topic=topic,
            active_message_count=active_count,
            inflight_message_count=group["pending"],
            config=SubscriptionConfig(),
            nref=subscription,
        )

    def list_subscriptions(self, topic: str, nargs: Any):
        result = []
        groups = self.client.xinfo_groups(name=topic)
        for group in groups:
            result.append(group["name"].decode("utf-8"))
        return result

    def batch(
        self,
        topic: str,
        batch: MessageBatch,
        nargs: Any,
    ):
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                topic,
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            self.client.xadd(**args)

    def check_pending_messages(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None,
        worker_id: str,
        visibility_timeout: float,
        nargs: Any,
    ):
        max_count = config.max_count if config and config.max_count else 1
        pending = self.client.xpending_range(
            topic, subscription, "-", "+", max_count
        )
        stale_ids = [
            entry["message_id"]
            for entry in pending
            if entry["idle"] > visibility_timeout * 1000
        ]
        if stale_ids:
            nresult = self.client.xclaim(
                topic, subscription, worker_id, visibility_timeout, *stale_ids
            )
            stream_key, msgs = nresult[0] if nresult else (None, [])
            return msgs
        return []

    def pull(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None,
        worker_id: str,
        visible_timeout: float,
        check_pending: bool,
        nargs: Any,
    ):
        msgs = []
        if check_pending:
            msgs = self.check_pending_messages(
                topic, subscription, config, worker_id, visible_timeout, nargs
            )
        if len(msgs) > 0 and config and config.max_count:
            new_config = config.copy()
            new_config.max_count = config.max_count - len(msgs)
        else:
            new_config = config
        args = self.op_converter.convert_pull(
            topic,
            subscription,
            new_config,
            worker_id,
        )
        nresult = self.client.xreadgroup(**args)
        stream_key, msgs = nresult[0] if nresult else (None, [])
        return msgs

    def nack(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        nargs: Any,
    ):
        fields = key.nref["fields"].copy()
        fields[b"origin_id"] = key.nref["origin_id"] or key.nref["id"]
        self.client.xadd(
            name=topic,
            fields=fields,
            id="*",
        )
        self.client.xack(
            topic,
            subscription,
            key.nref["id"],
        )

    def purge(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ):
        self.client.xgroup_destroy(topic, subscription)
        self.client.xgroup_create(topic, subscription, id="$", mkstream=True)

    def extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None,
        worker_id: str,
        nargs: Any,
    ):
        args = self.op_converter.convert_extend(
            topic,
            subscription,
            key,
            timeout,
            worker_id,
        )
        return self.client.xclaim(**args)

    def count(self, topic: str, last_id: str, batch_size: int = 1000):
        count = 0
        while True:
            entries = self.client.xrange(
                topic, min=f"({last_id}", max="+", count=batch_size
            )
            if not entries:
                break
            count += len(entries)
            last_id = list(entries)[-1][0].decode()
        return count

    def close(self, topic: str | None, nargs: Any):
        pass


class AsyncClientHelper:
    client: Any
    op_converter: OperationConverter

    def __init__(
        self,
        client: Any,
        op_converter: OperationConverter,
    ):
        self.client = client
        self.op_converter = op_converter

    async def create_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        try:
            await self.client.xgroup_create(
                name=queue, groupname=queue, id="$", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    async def update_queue(
        self,
        queue: str,
        config: QueueConfig | None,
        nargs: Any,
    ):
        if not await self.client.exists(queue):
            raise NotFoundError

    async def has_queue(
        self,
        queue: str,
        nargs: Any,
    ) -> bool:
        return await self.client.exists(queue) > 0

    async def get_queue(
        self,
        queue: str,
        nargs: Any,
    ) -> QueueInfo:
        if not await self.client.exists(queue):
            raise NotFoundError
        groups = await self.client.xinfo_groups(name=queue)
        group = next(
            (
                group
                for group in groups
                if group["name"].decode("utf-8") == queue
            ),
            None,
        )
        if not group:
            raise NotFoundError
        active_count = await self.count(
            queue, group["last-delivered-id"].decode("utf-8")
        )
        return QueueInfo(
            name=queue,
            active_message_count=active_count,
            inflight_message_count=group["pending"] if group else None,
            config=QueueConfig(),
            nref=queue,
        )

    async def create_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        if await self.client.exists(topic):
            if where_exists is False:
                raise ConflictError
            return
        try:
            await self.client.xgroup_create(
                name=topic, groupname="dummy", id="$", mkstream=True
            )
            await self.client.xgroup_destroy(
                name=topic,
                groupname="dummy",
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    async def update_topic(
        self,
        topic: str,
        config: TopicConfig | None,
        nargs: Any,
    ):
        if not await self.client.exists(topic):
            raise NotFoundError

    async def drop_topic(
        self,
        topic: str,
        where_exists: bool | None,
        nargs: Any,
    ):
        deleted = await self.client.delete(topic)
        if not deleted and where_exists is True:
            raise NotFoundError

    async def has_topic(
        self,
        topic: str,
        nargs: Any,
    ) -> bool:
        return await self.client.exists(topic) > 0

    async def get_topic(
        self,
        topic: str,
        nargs: Any,
    ) -> TopicInfo:
        if not await self.client.exists(topic):
            raise NotFoundError
        groups = await self.client.xinfo_groups(name=topic)
        return TopicInfo(
            name=topic,
            subscription_count=len(groups),
            config=TopicConfig(),
            nref=topic,
        )

    async def list_topics(self, nargs: Any):
        result = []
        cursor = 0
        while True:
            cursor, keys = await self.client.scan(
                cursor=cursor, _type="stream"
            )
            for key in keys:
                if key.startswith(b"__"):
                    continue
                result.append(key.decode("utf-8"))
            if cursor == 0:
                break
        return result

    async def create_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | None,
        where_exists: bool | None,
        nargs: Any,
    ):
        try:
            await self.client.xgroup_create(
                name=topic, groupname=subscription, id="$", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                if where_exists is False:
                    raise ConflictError
            else:
                raise

    async def update_subscription(
        self,
        topic: str,
        subscription: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ):
        if not await self.client.exists(topic):
            raise NotFoundError
        groups = await self.client.xinfo_groups(name=topic)
        if not any(
            group["name"].decode("utf-8") == subscription for group in groups
        ):
            raise NotFoundError

    async def drop_subscription(
        self,
        topic: str,
        subscription: str,
        where_exists: bool | None,
        nargs: Any,
    ):
        deleted = await self.client.xgroup_destroy(
            name=topic,
            groupname=subscription,
        )
        if not deleted and where_exists is True:
            raise NotFoundError

    async def has_subscription(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ):
        groups = await self.client.xinfo_groups(name=topic)
        return any(
            group["name"].decode("utf-8") == subscription for group in groups
        )

    async def get_subscription(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        if not await self.client.exists(topic):
            raise NotFoundError
        groups = await self.client.xinfo_groups(name=topic)
        group = next(
            (
                group
                for group in groups
                if group["name"].decode("utf-8") == subscription
            ),
            None,
        )
        if not group:
            raise NotFoundError
        active_count = await self.count(
            topic, group["last-delivered-id"].decode("utf-8")
        )
        return SubscriptionInfo(
            name=subscription,
            topic=topic,
            active_message_count=active_count,
            inflight_message_count=group["pending"],
            config=SubscriptionConfig(),
            nref=subscription,
        )

    async def list_subscriptions(self, topic: str, nargs: Any):
        result = []
        groups = await self.client.xinfo_groups(name=topic)
        for group in groups:
            result.append(group["name"].decode("utf-8"))
        return result

    async def batch(
        self,
        topic: str,
        batch: MessageBatch,
        nargs: Any,
    ):
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                topic,
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            await self.client.xadd(**args)

    async def check_pending_messages(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None,
        worker_id: str,
        visible_timeout: float,
        nargs: Any,
    ):
        max_count = config.max_count if config and config.max_count else 1
        pending = await self.client.xpending_range(
            topic, subscription, "-", "+", max_count
        )
        stale_ids = [
            entry["message_id"]
            for entry in pending
            if entry["idle"] > visible_timeout * 1000
        ]
        if stale_ids:
            nresult = await self.client.xclaim(
                topic, subscription, worker_id, visible_timeout, *stale_ids
            )
            stream_key, msgs = nresult[0] if nresult else (None, [])
            return msgs
        return []

    async def pull(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None,
        worker_id: str,
        visible_timeout: float,
        check_pending: bool,
        nargs: Any,
    ):
        msgs = []
        if check_pending:
            msgs = await self.check_pending_messages(
                topic, subscription, config, worker_id, visible_timeout, nargs
            )
        if len(msgs) > 0 and config and config.max_count:
            new_config = config.copy()
            new_config.max_count = config.max_count - len(msgs)
        else:
            new_config = config
        args = self.op_converter.convert_pull(
            topic,
            subscription,
            new_config,
            worker_id,
        )
        nresult = await self.client.xreadgroup(**args)
        stream_key, msgs = nresult[0] if nresult else (None, [])
        return msgs

    async def nack(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        nargs: Any,
    ):
        fields = key.nref["fields"].copy()
        fields[b"origin_id"] = key.nref["origin_id"] or key.nref["id"]
        await self.client.xadd(
            name=topic,
            fields=fields,
            id="*",
        )
        await self.client.xack(
            topic,
            subscription,
            key.nref["id"],
        )

    async def purge(
        self,
        topic: str,
        subscription: str,
        nargs: Any,
    ):
        await self.client.xgroup_destroy(topic, subscription)
        await self.client.xgroup_create(
            topic, subscription, id="$", mkstream=True
        )

    async def extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None,
        worker_id: str,
        nargs: Any,
    ):
        args = self.op_converter.convert_extend(
            topic,
            subscription,
            key,
            timeout,
            worker_id,
        )
        return await self.client.xclaim(**args)

    async def count(self, topic: str, last_id: str, batch_size: int = 1000):
        count = 0
        while True:
            entries = await self.client.xrange(
                topic, min=f"({last_id}", max="+", count=batch_size
            )
            if not entries:
                break
            count += len(entries)
            last_id = list(entries)[-1][0].decode()
        return count

    async def close(self, topic: str | None, nargs: Any):
        if not topic:
            await self.client.aclose()


class OperationConverter:
    def convert_put(
        self,
        topic: str | None,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict[str, Any]:
        if config is not None and config.delay is not None:
            raise BadRequestError("Delay is not supported for Redis Streams")
        fields: dict[str, Any] = {}
        if isinstance(value, bytes):
            fields["body"] = value
            content_type = "application/octet-stream"
        elif isinstance(value, str):
            fields["body"] = value
            content_type = "text/plain"
        elif isinstance(value, dict):
            fields["body"] = json.dumps(value)
            content_type = "application/json"
        elif isinstance(value, DataModel):
            fields["body"] = value.to_json()
            content_type = "application/json"
        else:
            raise BadRequestError("Message type not supported")

        if properties:
            if properties.message_id:
                fields["message_id"] = properties.message_id
            if properties.group_id:
                fields["group_id"] = properties.group_id
        fields["content_type"] = content_type

        if metadata:
            fields["metadata"] = json.dumps(metadata)

        return {
            "name": topic,
            "fields": fields,
            "id": "*",
        }

    def convert_pull(
        self,
        topic: str,
        subscription: str,
        config: MessagePullConfig | None,
        worker_id: str,
    ) -> dict[str, Any]:
        count = config.max_count if config and config.max_count else 1
        block_ms = 0
        if config and config.max_wait_time:
            block_ms = int(config.max_wait_time * 1000)
        return {
            "groupname": subscription,
            "consumername": worker_id,
            "streams": {topic: ">"},
            "count": count,
            "block": block_ms if block_ms > 0 else None,
        }

    def convert_ack(
        self,
        topic: str | None,
        subscription: str | None,
        key: MessageKey,
    ) -> list:
        return [topic, subscription, key.nref["id"]]

    def convert_extend(
        self,
        topic: str,
        subscription: str,
        key: MessageKey,
        timeout: int | None,
        worker_id: str,
    ) -> dict[str, Any]:
        return {
            "name": topic,
            "groupname": subscription,
            "consumername": worker_id,
            "min_idle_time": 0,
            "message_ids": [key.nref["id"]],
        }


class ResultConverter:
    def _convert_key(
        self,
        id: bytes,
        fields: dict[bytes, bytes],
        origin_id: bytes | None,
    ) -> MessageKey:
        return MessageKey(
            nref={
                "id": id,
                "fields": fields,
                "origin_id": origin_id,
            }
        )

    def _convert_message(
        self, id: bytes, fields: dict[bytes, bytes]
    ) -> MessageItem:
        def _b2s(b: bytes) -> str:
            return b.decode("utf-8")

        body_raw: Any = fields.get(b"body")
        content_type = _b2s(fields.get(b"content_type", b"text/plain"))
        metadata_raw = fields.get(b"metadata")
        fields.get(b"body", None)

        if content_type == "application/octet-stream":
            value: MessageValueType = body_raw
        elif content_type == "application/json":
            value = json.loads(body_raw)
        else:
            value = _b2s(body_raw)

        origin_id = fields.get(b"origin_id", None)
        metadata = json.loads(metadata_raw) if metadata_raw else None
        props = MessageProperties(
            message_id=_b2s(fields.get(b"message_id", b"")) or None,
            content_type=content_type,
            group_id=_b2s(fields.get(b"group_id", b"")) or None,
            enqueued_time=float(_b2s(origin_id or id).split("-")[0]) / 1000.0,
            delivery_count=None,
        )
        return MessageItem(
            key=self._convert_key(id, fields, origin_id),
            value=value,
            metadata=metadata,
            properties=props,
        )

    def convert_pull(self, nresult: Any) -> list[MessageItem]:
        if not nresult:
            return []
        result: list[MessageItem] = []
        for message_id, fields in nresult:
            result.append(self._convert_message(message_id, fields))
        return result
