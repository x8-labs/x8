from __future__ import annotations

import base64
import json
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.queue import QueueMessage, QueueProperties
from x8._common.azure_provider import AzureProvider
from x8.core import Context, DataModel, NCall, Operation, Response
from x8.core.exceptions import BadRequestError, ConflictError, NotFoundError
from x8.messaging._common import (
    MessageBatch,
    MessageItem,
    MessageKey,
    MessageProperties,
    MessagePullConfig,
    MessagePutConfig,
    MessageValueType,
    MessagingOperation,
    MessagingOperationParser,
    QueueConfig,
    QueueInfo,
)

from .._feature import QueueFeature


class AzureQueueStorage(AzureProvider):
    account_name: str | None
    queue: str | None
    connection_string: str | None
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _queue_cache: dict[str, AzureQueue]
    _aqueue_cache: dict[str, AzureQueue]
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        queue: str | None = None,
        connection_string: str | None = None,
        account_url: str | None = None,
        account_key: str | None = None,
        sas_token: str | None = None,
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
            queue:
                Queue name.
            connection_string:
                Azure storage connection string.
            account_url:
                Azure storage account url.
            account_key:
                Azure storage account key.
            sas_token:
                SAS token.
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
                Native parameters to Queue Storage client.
        """
        self.queue = queue
        self.connection_string = connection_string
        self.account_url = account_url
        self.account_key = account_key
        self.sas_token = sas_token
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._queue_cache = dict()
        self._aqueue_cache = dict()
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()

        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return feature not in [
            QueueFeature.BUILTIN_DLQ,
            QueueFeature.CONFIGURABLE_DLQ,
        ]

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from azure.storage.queue import QueueServiceClient

        (self._credential, self._client) = self._init_credential_client(
            QueueServiceClient, self._get_credential
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        from azure.storage.queue.aio import QueueServiceClient

        (
            self._acredential,
            self._aclient,
        ) = self._init_credential_client(
            QueueServiceClient, self._aget_credential
        )

    def _init_credential_client(
        self,
        service_client,
        get_credential,
    ):
        credential = None
        if self.connection_string is not None:
            client = service_client.from_connection_string(
                self.connection_string,
                **self.nparams,
            )
        elif self.account_key is not None:
            client = service_client(
                account_url=self.account_url,
                credential=self.account_key,
                **self.nparams,
            )
        elif self.sas_token is not None:
            client = service_client(
                account_url=self.account_url,
                credential=self.account_key,
                **self.nparams,
            )
        else:
            credential = get_credential()
            client = service_client(
                account_url=self.account_url,
                credential=credential,
                **self.nparams,
            )
        return credential, client

    def _get_queue_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        queue_name = op_parser.get_queue()
        queue_name = queue_name or self.queue or self.__component__.queue
        return queue_name

    def _get_queue(
        self, op_parser: MessagingOperationParser
    ) -> AzureQueue | None:
        if op_parser.is_resource_op():
            return None
        queue_name = self._get_queue_name(op_parser)
        if queue_name is None:
            raise BadRequestError("Queue name must be specified")
        if queue_name in self._queue_cache:
            return self._queue_cache[queue_name]

        client = self._client.get_queue_client(queue_name)
        queue = AzureQueue(
            client=client,
            helper=ClientHelper(client, self._op_converter),
        )
        self._queue_cache[queue_name] = queue
        return queue

    def _aget_queue(
        self, op_parser: MessagingOperationParser
    ) -> AzureQueue | None:
        if op_parser.is_resource_op():
            return None
        queue_name = self._get_queue_name(op_parser)
        if queue_name is None:
            raise BadRequestError("Queue name must be specified")
        if queue_name in self._aqueue_cache:
            return self._aqueue_cache[queue_name]
        client = self._aclient.get_queue_client(queue_name)
        queue = AzureQueue(
            client=client,
            helper=AsyncClientHelper(client, self._op_converter),
        )
        self._aqueue_cache[queue_name] = queue
        return queue

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = MessagingOperationParser(operation)
        queue = None
        resource_helper = None
        if not op_parser.is_resource_op():
            queue = self._get_queue(op_parser)
        resource_helper = ResourceHelper(
            self._client,
            self._queue_cache,
        )
        ncall = self._get_ncall(
            op_parser,
            queue,
            self._client,
            resource_helper,
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
        queue = None
        resource_helper = None
        if not op_parser.is_resource_op():
            queue = self._aget_queue(op_parser)
        resource_helper = AsyncResourceHelper(
            self._aclient,
            self._aqueue_cache,
        )
        ncall = self._get_ncall(
            op_parser,
            queue,
            self._aclient,
            resource_helper,
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
        queue: AzureQueue | None,
        service_client: Any,
        resource_helper: Any,
    ) -> NCall | None:
        if queue is not None:
            queue_client = queue.client
            helper = queue.helper
        op_converter = self._op_converter
        call = None
        nargs = op_parser.get_nargs()

        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args = op_converter.convert_create_queue(
                queue_name=self._get_queue_name(op_parser),
                config=op_parser.get_queue_config(),
            )
            where_exists = op_parser.get_where_exists()
            call = NCall(
                service_client.create_queue,
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
                "queue_name": self._get_queue_name(op_parser),
                "config": op_parser.get_queue_config(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.update_queue,
                args,
                nargs,
            )
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            args = {
                "queue_name": self._get_queue_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            where_exists = op_parser.get_where_exists()
            call = NCall(
                resource_helper.drop_queue,
                args,
                nargs,
                {
                    ResourceNotFoundError: (
                        NotFoundError if where_exists is True else None
                    )
                },
            )
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            args = {
                "queue_name": self._get_queue_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.has_queue,
                args,
                nargs,
            )
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
            args = {
                "queue_name": self._get_queue_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.get_queue,
                args,
                nargs,
            )
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_queues, args)
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = op_converter.convert_put(
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                queue_client.send_message,
                args,
                nargs,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "batch": op_parser.get_batch(),
                "nargs": nargs,
            }
            call = NCall(
                helper.batch,
                args,
                nargs,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = {
                "config": op_parser.get_pull_config(),
                "nargs": nargs,
            }
            call = NCall(
                helper.pull,
                args,
                nargs,
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = op_converter.convert_ack(op_parser.get_key())
            call = NCall(
                queue_client.delete_message,
                args,
                nargs,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = op_converter.convert_nack(
                op_parser.get_key(),
            )
            call = NCall(
                queue_client.update_message,
                args,
                nargs,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = op_converter.convert_extend(
                op_parser.get_key(),
                op_parser.get_timeout(),
            )
            call = NCall(
                queue_client.update_message,
                args,
                nargs,
            )
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            call = NCall(
                queue_client.clear_messages,
                None,
                nargs,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {
                "queue_name": op_parser.get_queue(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.close, args)
        return call

    def _convert_nresult(
        self,
        nresult: Any,
        op_parser: MessagingOperationParser,
    ) -> Any:
        result_converter = ResultConverter()
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
            result = result_converter.convert_extend(nresult)
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

    def __init__(self, client: Any, op_converter: OperationConverter):
        self.client = client
        self.op_converter = op_converter

    def batch(self, batch: MessageBatch, nargs: Any) -> None:
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            NCall(
                self.client.send_message,
                args,
                nargs,
            ).invoke()

    def pull(self, config: MessagePullConfig | None, nargs: Any) -> list:
        args = self.op_converter.convert_pull(config)
        nresult = NCall(
            self.client.receive_messages,
            args,
            nargs,
        ).invoke()
        return nresult


class AsyncClientHelper:
    client: Any
    op_converter: OperationConverter

    def __init__(self, client: Any, op_converter: OperationConverter):
        self.client = client
        self.op_converter = op_converter

    async def batch(self, batch: MessageBatch, nargs: Any) -> None:
        for operation in batch.operations:
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            await NCall(
                self.client.send_message,
                args,
                nargs,
            ).ainvoke()

    async def pull(self, config: MessagePullConfig | None, nargs: Any) -> list:
        args = self.op_converter.convert_pull(config)
        response = NCall(
            self.client.receive_messages,
            args,
            nargs,
        ).invoke()
        nresult = []
        async for message in response:
            nresult.append(message)
        return nresult


class ResourceHelper:
    client: Any
    queue_cache: dict[str, AzureQueue]

    def __init__(self, client: Any, queue_cache: dict):
        self.client = client
        self.queue_cache = queue_cache

    def update_queue(
        self,
        queue_name: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            queue_client.get_queue_properties()
        except ResourceNotFoundError:
            raise NotFoundError

    def drop_queue(
        self,
        queue_name: str,
        where_exists: bool,
        nargs: Any,
    ) -> None:
        if where_exists:
            try:
                queue_client = self.client.get_queue_client(queue_name)
                queue_client.get_queue_properties()
            except ResourceNotFoundError:
                raise NotFoundError
        NCall(
            self.client.delete_queue,
            {"queue": queue_name},
            nargs,
        ).invoke()

    def has_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> bool:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            queue_client.get_queue_properties()
            return True
        except ResourceNotFoundError:
            return False

    def get_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> QueueInfo:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            qp: QueueProperties = queue_client.get_queue_properties()
            return QueueInfo(
                name=queue_name,
                active_message_count=qp.approximate_message_count,
                nref=qp.name,
                config=QueueConfig(
                    nconfig=qp.metadata,
                ),
            )
        except ResourceNotFoundError:
            raise NotFoundError

    def list_queues(self, nargs: Any) -> list[str]:
        queues = NCall(self.client.list_queues, None, nargs).invoke()
        return [queue.name for queue in queues]

    def close(self, queue_name: str | None, nargs: Any) -> None:
        if queue_name is not None:
            if queue_name in self.queue_cache:
                queue = self.queue_cache[queue_name]
                queue.client.close()
        else:
            for queue in self.queue_cache.values():
                queue.client.close()
            self.queue_cache.clear()
            self.client.close()


class AsyncResourceHelper:
    client: Any
    queue_cache: dict[str, AzureQueue]

    def __init__(self, client: Any, queue_cache: dict):
        self.client = client
        self.queue_cache = queue_cache

    async def update_queue(
        self,
        queue_name: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            await queue_client.get_queue_properties()
        except ResourceNotFoundError:
            raise NotFoundError

    async def drop_queue(
        self,
        queue_name: str,
        where_exists: bool,
        nargs: Any,
    ) -> None:
        if where_exists:
            try:
                queue_client = self.client.get_queue_client(queue_name)
                await queue_client.get_queue_properties()
            except ResourceNotFoundError:
                raise NotFoundError
        await NCall(
            self.client.delete_queue,
            {"queue": queue_name},
            nargs,
        ).ainvoke()

    async def has_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> bool:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            await queue_client.get_queue_properties()
            return True
        except ResourceNotFoundError:
            return False

    async def get_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> QueueInfo:
        try:
            queue_client = self.client.get_queue_client(queue_name)
            qp: QueueProperties = await queue_client.get_queue_properties()
            return QueueInfo(
                name=queue_name,
                active_message_count=qp.approximate_message_count,
                nref=qp.name,
                config=QueueConfig(
                    nconfig=qp.metadata,
                ),
            )
        except ResourceNotFoundError:
            raise NotFoundError

    async def list_queues(self, nargs: Any) -> list[str]:
        queues = NCall(self.client.list_queues, None, nargs).invoke()
        return [queue.name async for queue in queues]

    async def close(self, queue_name: str | None, nargs: Any) -> None:
        if queue_name is not None:
            if queue_name in self.queue_cache:
                queue = self.queue_cache[queue_name]
                await queue.client.close()
                self.queue_cache.pop(queue_name)
        else:
            for queue in self.queue_cache.values():
                await queue.client.close()
            self.queue_cache.clear()
            await self.client.close()


class OperationConverter:
    def convert_create_queue(
        self,
        queue_name: str | None,
        config: QueueConfig | None,
    ) -> dict:
        args: dict = {"name": queue_name}
        if config and config.nconfig:
            args = args | config.nconfig
        return args

    def convert_put(
        self,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict:
        content_type = None
        visibility_timeout = 0
        body: str | None = None

        if isinstance(value, str):
            body = value
            content_type = "text/plain"
        elif isinstance(value, bytes):
            body = base64.b64encode(value).decode("utf-8")
            content_type = "application/octet-stream"
        elif isinstance(value, dict):
            body = json.dumps(value)
            content_type = "application/json"
        elif isinstance(value, DataModel):
            body = value.to_json()
            content_type = "application/json"
        else:
            raise BadRequestError("Message type not supported")

        content: dict = {
            "body": body,
            "content_type": content_type,
        }

        if metadata:
            content["metadata"] = metadata
        if properties:
            if properties.message_id:
                content["message_id"] = properties.message_id
            if properties.group_id:
                content["group_id"] = properties.group_id

        if config is not None and config.delay is not None:
            visibility_timeout = config.delay

        return {
            "content": json.dumps(content),
            "visibility_timeout": visibility_timeout,
        }

    def convert_pull(self, config: MessagePullConfig | None) -> dict:
        args: dict = {}
        if config and config.max_count is not None:
            args["messages_per_page"] = config.max_count
        else:
            args["messages_per_page"] = 1
        if config and config.visibility_timeout:
            args["visibility_timeout"] = config.visibility_timeout
        return args

    def convert_ack(self, key: MessageKey) -> dict:
        message_data = key.nref
        return {
            "message": message_data["id"],
            "pop_receipt": message_data["pop_receipt"],
        }

    def convert_nack(self, key: MessageKey) -> dict:
        message_data = key.nref
        return {
            "message": message_data["id"],
            "pop_receipt": message_data["pop_receipt"],
            "visibility_timeout": 0,
            "content": message_data["content"],
        }

    def convert_extend(self, key: MessageKey, timeout: int | None) -> dict:
        message_data = key.nref
        return {
            "message": message_data["id"],
            "pop_receipt": message_data["pop_receipt"],
            "visibility_timeout": timeout,
            "content": message_data["content"],
        }


class ResultConverter:
    def _convert_key(self, message: QueueMessage) -> MessageKey:
        return MessageKey(
            id=message.id,
            nref={
                "id": message.id,
                "pop_receipt": message.pop_receipt,
                "content": message.content,
            },
        )

    def _convert_value(self, content: dict) -> MessageValueType:
        content_type = "text/plain"
        content_type = content.get("content_type", "text/plain")
        if content_type == "text/plain":
            return content.get("body", None)
        elif content_type == "application/octet-stream":
            return base64.b64decode(content.get("body", None))
        elif content_type == "application/json":
            return json.loads(content.get("body", None))
        return content.get("body", None)

    def _convert_metadata(self, content: dict) -> dict | None:
        if "metadata" in content:
            return content["metadata"]
        return None

    def _convert_properties(
        self,
        message: QueueMessage,
        content: dict,
    ) -> MessageProperties:
        return MessageProperties(
            message_id=content.get("message_id", None),
            group_id=content.get("group_id", None),
            content_type=content.get("content_type", "text/plain"),
            enqueued_time=(
                message.inserted_on.timestamp()
                if message.inserted_on
                else None
            ),
            delivery_count=message.dequeue_count,
        )

    def _convert_message(self, message: QueueMessage) -> MessageItem:
        content = json.loads(message.content)
        return MessageItem(
            key=self._convert_key(message=message),
            value=self._convert_value(content=content),
            metadata=self._convert_metadata(content=content),
            properties=self._convert_properties(
                message=message, content=content
            ),
        )

    def convert_pull(self, messages: list[QueueMessage]) -> list[MessageItem]:
        result = []
        for message in messages:
            result.append(self._convert_message(message))
        return result

    def convert_extend(self, message: QueueMessage) -> MessageItem:
        return MessageItem(key=self._convert_key(message=message))


class AzureQueue:
    client: Any
    helper: Any

    def __init__(self, client: Any, helper: Any):
        self.client = client
        self.helper = helper
