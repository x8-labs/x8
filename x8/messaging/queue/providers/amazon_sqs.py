from __future__ import annotations

import base64
import json
from typing import Any

import boto3
from botocore.exceptions import ClientError
from x8.core import Context, DataModel, NCall, Operation, Provider, Response
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


class AmazonSQS(Provider):
    queue: str | None
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    nparams: dict[str, Any]

    _sqs_client: Any
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _queue_url_cache: dict[str, str]

    def __init__(
        self,
        queue: str | None = None,
        region: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            queue:
                SQS queue name.
            region_name:
                AWS region name.
            profile_name:
                AWS profile name.
            aws_access_key_id:
                AWS access key ID.
            aws_secret_access_key:
                AWS secret access key.
            aws_session_token:
                AWS session token.
            nparams:
                Native parameters to SQS client.
        """
        self.queue = queue
        self.region = region
        self.profile_name = profile_name
        self.access_key_id = aws_access_key_id
        self.secret_access_key = aws_secret_access_key
        self.session_token = aws_session_token
        self.nparams = nparams

        self._sqs_client = None
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        self._queue_url_cache = dict()
        super().__init__(**kwargs)

    def __supports__(self, feature: str) -> bool:
        return feature not in [QueueFeature.BUILTIN_DLQ]

    def __setup__(self, context: Context | None = None) -> None:
        if self._sqs_client is not None:
            return

        session_kwargs = {}
        if self.profile_name:
            session_kwargs["profile_name"] = self.profile_name

        client_kwargs = {"region_name": self.region, **self.nparams}

        if self.access_key_id and self.secret_access_key:
            client_kwargs["aws_access_key_id"] = self.access_key_id
            client_kwargs["aws_secret_access_key"] = self.secret_access_key
            if self.session_token:
                client_kwargs["aws_session_token"] = self.session_token

        session = boto3.Session(**session_kwargs)
        self._sqs_client = session.client("sqs", **client_kwargs)

    def _get_queue_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        queue_name = op_parser.get_queue()
        queue_name = queue_name or self.queue or self.__component__.queue
        return queue_name

    def _get_queue_url(self, op_parser: MessagingOperationParser) -> str:
        queue_name = self._get_queue_name(op_parser)
        if queue_name is None:
            raise BadRequestError("Queue name is required")

        if queue_name in self._queue_url_cache:
            return self._queue_url_cache[queue_name]

        try:
            response = self._sqs_client.get_queue_url(QueueName=queue_name)
            self._queue_url_cache[queue_name] = response["QueueUrl"]
            return response["QueueUrl"]
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                raise BadRequestError(f"Queue {queue_name} does not exist")
            raise

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
                self._sqs_client,
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

        # CREATE QUEUE
        if op_parser.op_equals(MessagingOperation.CREATE_QUEUE):
            args: dict = {
                "queue_name": self._get_queue_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "config": op_parser.get_queue_config(),
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
                "queue_name": self._get_queue_name(op_parser),
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
                "queue_name": self._get_queue_name(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.drop_queue,
                args,
                None,
            )
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            args = {}
            call = NCall(
                self._sqs_client.list_queues,
                args,
                nargs,
            )
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            args = {
                "queue_name": self._get_queue_name(op_parser),
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
                "queue_name": self._get_queue_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_queue,
                args,
                None,
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = self._op_converter.convert_put(
                self._get_queue_url(op_parser),
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                self._sqs_client.send_message,
                args,
                nargs,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "batch": op_parser.get_batch(),
                "queue_url": self._get_queue_url(op_parser),
            }
            call = NCall(
                client_helper.batch,
                args,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = self._op_converter.convert_pull(
                self._get_queue_url(op_parser),
                op_parser.get_pull_config(),
            )
            call = NCall(
                self._sqs_client.receive_message,
                args,
                nargs,
            )
        # ACK
        elif op_parser.op_equals(MessagingOperation.ACK):
            args = {
                "QueueUrl": self._get_queue_url(op_parser),
                "ReceiptHandle": op_parser.get_key().nref,
            }
            call = NCall(
                self._sqs_client.delete_message,
                args,
                nargs,
            )
        # NACK
        elif op_parser.op_equals(MessagingOperation.NACK):
            args = {
                "QueueUrl": self._get_queue_url(op_parser),
                "ReceiptHandle": op_parser.get_key().nref,
                "VisibilityTimeout": 0,
            }
            call = NCall(
                self._sqs_client.change_message_visibility,
                args,
                nargs,
            )
        # EXTEND
        elif op_parser.op_equals(MessagingOperation.EXTEND):
            args = {
                "QueueUrl": self._get_queue_url(op_parser),
                "ReceiptHandle": op_parser.get_key().nref,
                "VisibilityTimeout": op_parser.get_timeout(),
            }
            call = NCall(
                self._sqs_client.change_message_visibility,
                args,
                nargs,
            )
        # PURGE
        elif op_parser.op_equals(MessagingOperation.PURGE):
            args = {
                "queue_url": self._get_queue_url(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.purge,
                args,
            )
        # CLOSE
        elif op_parser.op_equals(MessagingOperation.CLOSE):
            args = {}
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
            result = nresult
            queue_name = self._get_queue_name(op_parser)
            if queue_name:
                self._queue_url_cache.pop(queue_name, None)
        # DROP QUEUE
        elif op_parser.op_equals(MessagingOperation.DROP_QUEUE):
            result = None
            queue_name = self._get_queue_name(op_parser)
            if queue_name:
                self._queue_url_cache.pop(queue_name, None)
        # LIST QUEUES
        elif op_parser.op_equals(MessagingOperation.LIST_QUEUES):
            result = result_converter.convert_list_queues(nresult)
        # HAS QUEUE
        elif op_parser.op_equals(MessagingOperation.HAS_QUEUE):
            result = nresult
        # GET QUEUE
        elif op_parser.op_equals(MessagingOperation.GET_QUEUE):
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
    sqs_client: Any
    op_converter: OperationConverter

    def __init__(
        self,
        sqs_client: Any,
        op_converter: OperationConverter,
    ):
        self.sqs_client = sqs_client
        self.op_converter = op_converter

    def create_queue(
        self,
        queue_name: str,
        where_exists: bool | None,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        try:
            NCall(
                self.sqs_client.get_queue_url,
                {"QueueName": queue_name},
            ).invoke()
            if where_exists is False:
                raise ConflictError(f"Queue {queue_name} already exists")
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                attributes = self.op_converter.convert_queue_attributes(
                    config=config,
                )
                NCall(
                    self.sqs_client.create_queue,
                    {
                        "QueueName": queue_name,
                        "Attributes": attributes,
                    },
                    nargs,
                ).invoke()
            else:
                raise

    def update_queue(
        self,
        queue_name: str,
        config: QueueConfig | None,
        nargs: Any,
    ) -> None:
        try:
            queue_url = NCall(
                self.sqs_client.get_queue_url,
                {"QueueName": queue_name},
            ).invoke()["QueueUrl"]
            attributes = self.op_converter.convert_queue_attributes(
                config=config
            )
            NCall(
                self.sqs_client.set_queue_attributes,
                {
                    "QueueUrl": queue_url,
                    "Attributes": attributes,
                },
                nargs,
            ).invoke()
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                raise NotFoundError(f"Queue {queue_name} does not exist")
            else:
                raise

    def drop_queue(
        self, queue_name: str, where_exists: bool | None, nargs: Any
    ) -> None:
        try:
            queue_url = NCall(
                self.sqs_client.get_queue_url,
                {"QueueName": queue_name},
            ).invoke()["QueueUrl"]
            NCall(
                self.sqs_client.delete_queue,
                {"QueueUrl": queue_url},
                nargs,
            ).invoke()
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                if where_exists is True:
                    raise NotFoundError(f"Queue {queue_name} does not exist")
            else:
                raise

    def has_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> bool:
        try:
            NCall(
                self.sqs_client.get_queue_url,
                {"QueueName": queue_name},
                nargs,
            ).invoke()
            return True
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                return False
            raise

    def get_queue(
        self,
        queue_name: str,
        nargs: Any,
    ) -> QueueInfo:
        try:
            res = NCall(
                self.sqs_client.get_queue_url,
                {"QueueName": queue_name},
                nargs,
            ).invoke()
            queue_url = res["QueueUrl"]
            res = NCall(
                self.sqs_client.get_queue_attributes,
                {
                    "QueueUrl": queue_url,
                    "AttributeNames": [
                        "All",
                    ],
                },
                nargs,
            ).invoke()
            attributes = res["Attributes"]
            fifo = attributes.get("FifoQueue", "false").lower() == "true"
            queue_arn = attributes.get("QueueArn")
            max_receive_count = None
            dlq_nref = None
            if "RedrivePolicy" in attributes:
                redrive_policy = json.loads(attributes["RedrivePolicy"])
                dlq_nref = redrive_policy.get("deadLetterTargetArn")
                if "maxReceiveCount" in redrive_policy:
                    max_receive_count = int(
                        redrive_policy.get("maxReceiveCount", 0)
                    )
            return QueueInfo(
                name=queue_name,
                active_message_count=int(
                    attributes.get("ApproximateNumberOfMessages", 0)
                ),
                inflight_message_count=int(
                    attributes.get("ApproximateNumberOfMessagesNotVisible", 0)
                ),
                scheduled_message_count=int(
                    attributes.get("ApproximateNumberOfMessagesDelayed", 0)
                ),
                config=QueueConfig(
                    visibility_timeout=int(
                        attributes.get("VisibilityTimeout", 0)
                    ),
                    ttl=int(attributes.get("MessageRetentionPeriod", 0)),
                    max_delivery_count=max_receive_count,
                    dlq_nref=dlq_nref,
                    fifo=fifo,
                ),
                nref=queue_arn,
            )
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                raise NotFoundError(f"Queue {queue_name} does not exist")
            raise

    def batch(
        self,
        batch: MessageBatch,
        queue_url: str,
    ) -> list:
        entries = []
        for i, operation in enumerate(batch.operations):
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                queue_url,
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            entry = {
                "Id": str(i),
                "MessageBody": args["MessageBody"],
            }

            if "MessageAttributes" in args:
                entry["MessageAttributes"] = args["MessageAttributes"]

            if "DelaySeconds" in args:
                entry["DelaySeconds"] = args["DelaySeconds"]

            if "MessageGroupId" in args:
                entry["MessageGroupId"] = args["MessageGroupId"]

            if "MessageDeduplicationId" in args:
                entry["MessageDeduplicationId"] = args[
                    "MessageDeduplicationId"
                ]

            entries.append(entry)

        response = NCall(
            self.sqs_client.send_message_batch,
            {"QueueUrl": queue_url, "Entries": entries},
        ).invoke()

        message_ids = []
        for msg in response.get("Successful", []):
            message_ids.append(msg.get("MessageId"))

        if "Failed" in response and response["Failed"]:
            for failed in response["Failed"]:
                raise BadRequestError(
                    (
                        f"Failed to send message {failed['Id']}: "
                        f"{failed['Message']}"
                    )
                )
        return message_ids

    def purge(
        self,
        queue_url: str,
        nargs: Any,
    ) -> None:
        try:
            NCall(
                self.sqs_client.purge_queue,
                {"QueueUrl": queue_url},
                nargs,
            ).invoke()
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "AWS.SimpleQueueService.PurgeQueueInProgress"
            ):
                pass

    def close(self) -> Any:
        return None


class OperationConverter:
    def convert_queue_attributes(
        self,
        config: QueueConfig | None,
    ) -> dict:
        attributes = {}
        if config:
            if config.visibility_timeout:
                attributes["VisibilityTimeout"] = str(
                    int(config.visibility_timeout)
                )
            if config.ttl:
                attributes["MessageRetentionPeriod"] = str(int(config.ttl))
            if config.fifo:
                attributes["FifoQueue"] = "true"
            if config.dlq_nref and config.max_delivery_count:
                redrive_policy = {
                    "deadLetterTargetArn": config.dlq_nref,
                    "maxReceiveCount": str(config.max_delivery_count),
                }
                attributes["RedrivePolicy"] = json.dumps(redrive_policy)
            if config.nconfig:
                attributes.update(config.nconfig)
        return attributes

    def convert_put(
        self,
        queue_url: str,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict:
        message_body: str = ""
        message_attributes: dict = {}
        if isinstance(value, str):
            message_body = value
            content_type = "text/plain"
        elif isinstance(value, bytes):
            message_body = base64.b64encode(value).decode("utf-8")
            content_type = "application/octet-stream"
        elif isinstance(value, dict):
            message_body = json.dumps(value)
            content_type = "application/json"
        elif isinstance(value, DataModel):
            message_body = value.to_json()
            content_type = "application/json"
        else:
            raise BadRequestError("Message type not supported")

        message_attributes["content_type"] = {
            "DataType": "String",
            "StringValue": content_type,
        }

        if metadata:
            for key, val in metadata.items():
                if isinstance(val, str):
                    message_attributes[key] = {
                        "DataType": "String",
                        "StringValue": val,
                    }
                elif isinstance(val, (int, float)):
                    message_attributes[key] = {
                        "DataType": "Number",
                        "StringValue": str(val),
                    }
                elif isinstance(val, bytes):
                    message_attributes[key] = {
                        "DataType": "Binary",
                        "BinaryValue": val,
                    }
                else:
                    message_attributes[key] = {
                        "DataType": "String",
                        "StringValue": str(val),
                    }

        message_deduplication_id = None
        message_group_id = None
        if properties:
            if properties.message_id:
                message_attributes["message_id"] = {
                    "DataType": "String",
                    "StringValue": properties.message_id,
                }
                message_deduplication_id = properties.message_id

            if properties.group_id:
                message_attributes["group_id"] = {
                    "DataType": "String",
                    "StringValue": properties.group_id,
                }
                message_group_id = properties.group_id

        args: dict = {
            "QueueUrl": queue_url,
            "MessageBody": message_body,
        }

        if message_attributes:
            args["MessageAttributes"] = message_attributes

        if message_group_id and queue_url.endswith(".fifo"):
            args["MessageGroupId"] = message_group_id

        if message_deduplication_id and queue_url.endswith(".fifo"):
            args["MessageDeduplicationId"] = message_deduplication_id

        if config and config.delay is not None:
            args["DelaySeconds"] = min(900, int(config.delay))
        return args

    def convert_pull(
        self, queue_url: str, config: MessagePullConfig | None
    ) -> dict:
        args: dict = {
            "QueueUrl": queue_url,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        if config:
            if config.max_wait_time is not None:
                args["WaitTimeSeconds"] = int(config.max_wait_time)

            if config.max_count is not None:
                args["MaxNumberOfMessages"] = min(10, config.max_count)

            if config.visibility_timeout is not None:
                args["VisibilityTimeout"] = int(config.visibility_timeout)

        return args


class ResultConverter:
    def _convert_key(self, message: dict) -> MessageKey:
        return MessageKey(nref=message.get("ReceiptHandle"))

    def _convert_value(self, message: dict) -> MessageValueType:
        message_attributes = message.get("MessageAttributes", {})
        content_type = message_attributes.get(
            "content_type", {"StringValue": "text/plain"}
        ).get("StringValue", "text/plain")

        body = message.get("Body", "")

        if content_type == "application/octet-stream":
            return base64.b64decode(body)
        elif content_type == "application/json":
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return body

        return body

    def _convert_metadata(self, message: dict) -> dict | None:
        message_attributes = message.get("MessageAttributes", {})
        if not message_attributes:
            return None

        metadata = {}
        for key, attr in message_attributes.items():
            if key in ["content_type", "message_id", "group_id"]:
                continue

            if attr.get("DataType") == "String":
                metadata[key] = attr.get("StringValue")
            elif attr.get("DataType") == "Number":
                try:
                    metadata[key] = float(attr.get("StringValue"))
                    if metadata[key].is_integer():
                        metadata[key] = int(metadata[key])
                except (ValueError, TypeError):
                    metadata[key] = attr.get("StringValue")
            elif attr.get("DataType") == "Binary":
                metadata[key] = attr.get("BinaryValue")
            else:
                metadata[key] = attr.get("StringValue")

        return metadata if metadata else None

    def _convert_properties(self, message: dict) -> MessageProperties:
        message_attributes = message.get("MessageAttributes", {})
        attributes = message.get("Attributes", {})

        message_id = None
        if "message_id" in message_attributes:
            message_id = message_attributes["message_id"].get("StringValue")

        group_id = None
        if "group_id" in message_attributes:
            group_id = message_attributes["group_id"].get("StringValue")

        content_type = "text/plain"
        if "content_type" in message_attributes:
            content_type = message_attributes["content_type"].get(
                "StringValue", "text/plain"
            )

        enqueued_time = None
        if "SentTimestamp" in attributes:
            try:
                enqueued_time = float(attributes["SentTimestamp"]) / 1000.0
            except (ValueError, TypeError):
                pass

        # Get other properties
        return MessageProperties(
            message_id=message_id or message.get("MessageId"),
            content_type=content_type,
            group_id=group_id,
            enqueued_time=enqueued_time,
            delivery_count=(
                int(attributes.get("ApproximateReceiveCount", 1))
                if attributes
                else 1
            ),
        )

    def _convert_message(
        self,
        message: dict,
    ) -> MessageItem:
        return MessageItem(
            key=self._convert_key(message=message),
            value=self._convert_value(message=message),
            metadata=self._convert_metadata(message=message),
            properties=self._convert_properties(message=message),
        )

    def convert_list_queues(self, nresult: dict) -> list[str]:
        queues = []
        for queue_url in nresult.get("QueueUrls", []):
            queue_name = queue_url.split("/")[-1]
            queues.append(queue_name)
        return queues

    def convert_pull(self, nresult: dict) -> list[MessageItem]:
        if not nresult or "Messages" not in nresult:
            return []

        result = []
        for message in nresult["Messages"]:
            result.append(self._convert_message(message))
        return result
