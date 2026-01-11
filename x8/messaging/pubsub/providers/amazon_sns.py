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
    SubscriptionConfig,
    SubscriptionInfo,
    TopicConfig,
    TopicInfo,
)

from .._feature import PubSubFeature


class AmazonSNSBase(Provider):
    topic: str | None
    subscription: str | None
    region: str | None
    profile_name: str | None
    account_id: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    create_drop_queue: bool
    nparams: dict[str, Any]

    _session: Any
    _sns_client: Any
    _sqs_client: Any
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _account_id: str | None
    _queue_url_cache: dict[str, str]

    def __init__(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        region: str | None = None,
        account_id: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        create_drop_queue: bool = True,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            topic:
                SNS topic name or ARN.
            subscription:
                SQS subscription name or ARN.
            region_name:
                AWS region name.
            account_id:
                AWS account ID.
            profile_name:
                AWS profile name.
            aws_access_key_id:
                AWS access key ID.
            aws_secret_access_key:
                AWS secret access key.
            aws_session_token:
                AWS session token.
            create_drop_queue:
                Whether to create and drop the queue
                on creating and dropping subscriptions.
                Defaults to True.
            nparams:
                Native parameters to AWS clients.
        """
        self.topic = topic
        self.subscription = subscription
        self.region = region
        self.account_id = account_id
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.create_drop_queue = create_drop_queue
        self.nparams = nparams

        self._sns_client = None
        self._sqs_client = None
        self._account_id = None
        self._queue_url_cache = dict()

        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        super().__init__(**kwargs)

    def __supports__(self, feature: str) -> bool:
        return feature not in [PubSubFeature.BUILTIN_DLQ]

    def __setup__(self, context: Context | None = None) -> None:
        if self._sns_client is not None:
            return

        self._session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            profile_name=self.profile_name,
            region_name=self.region,
        )

        self._sns_client = self._session.client("sns", **self.nparams)
        self._sqs_client = self._session.client("sqs", **self.nparams)

    def _get_topic_arn(self, op_parser: MessagingOperationParser) -> str:
        topic_name = self._get_topic_name(op_parser)
        if topic_name is None:
            raise BadRequestError("Topic name is required")

        if topic_name.startswith("arn:aws:sns:"):
            return topic_name

        account_id = self._get_account_id()
        return f"arn:aws:sns:{self.region}:{account_id}:{topic_name}"

    def _get_subscription_endpoint(
        self, op_parser: MessagingOperationParser
    ) -> str:
        subscription_name = self._get_subscription_name(op_parser)
        if subscription_name is None:
            raise BadRequestError("Subscription name is required")

        if subscription_name.startswith("arn:aws:"):
            return subscription_name

        account_id = self._get_account_id()
        return f"arn:aws:sqs:{self.region}:{account_id}:{subscription_name}"

    def _get_account_id(self) -> str:
        if self._account_id is not None:
            return self._account_id
        sts_client = self._session.client("sts", region_name=self.region)
        response = sts_client.get_caller_identity()
        self._account_id = response["Account"]
        return self._account_id

    def _get_topic_name(
        self, op_parser: MessagingOperationParser
    ) -> str | None:
        topic_name = op_parser.get_topic()
        topic_name = topic_name or self.topic or self.__component__.topic
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

    def _get_queue_url(self, op_parser: MessagingOperationParser) -> str:
        subscription_name = self._get_subscription_name(op_parser)
        if subscription_name is None:
            raise BadRequestError("Subscription name is required")

        if subscription_name.startswith("arn:aws:sqs:"):
            queue_name = subscription_name.split(":")[-1]
        else:
            queue_name = subscription_name

        if queue_name in self._queue_url_cache:
            return self._queue_url_cache[queue_name]

        response = self._sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response["QueueUrl"]
        self._queue_url_cache[queue_name] = queue_url
        return queue_url

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
                self._sns_client,
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
        op_converter = self._op_converter

        # CREATE TOPIC
        if op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            args: dict = {
                "topic_name": self._get_topic_name(op_parser),
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
                "topic_arn": self._get_topic_name(op_parser),
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
                "topic_arn": self._get_topic_arn(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.drop_topic,
                args,
            )
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            args = {
                "topic_arn": self._get_topic_arn(op_parser),
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
                "topic_arn": self._get_topic_arn(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_topic,
                args,
                None,
            )
        # LIST TOPICS
        elif op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            call = NCall(self._sns_client.list_topics, {}, nargs)
        # CREATE SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.CREATE_SUBSCRIPTION):
            args = {
                "topic_arn": self._get_topic_arn(op_parser),
                "protocol": "sqs",
                "endpoint": self._get_subscription_endpoint(op_parser),
                "config": op_parser.get_subscription_config(),
                "where_exists": op_parser.get_where_exists(),
                "create_drop_queue": self.create_drop_queue,
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
                "topic_arn": self._get_topic_arn(op_parser),
                "endpoint": self._get_subscription_endpoint(op_parser),
                "queue_url": self._get_queue_url(op_parser),
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
                "topic_arn": self._get_topic_arn(op_parser),
                "endpoint": self._get_subscription_endpoint(op_parser),
                "queue_url": self._get_queue_url(op_parser),
                "where_exists": op_parser.get_where_exists(),
                "create_drop_queue": self.create_drop_queue,
                "nargs": nargs,
            }
            call = NCall(
                client_helper.drop_subscription,
                args,
                None,
            )
        # LIST SUBSCRIPTIONS
        elif op_parser.op_equals(MessagingOperation.LIST_SUBSCRIPTIONS):
            args = {
                "TopicArn": self._get_topic_arn(op_parser),
            }
            call = NCall(
                self._sns_client.list_subscriptions_by_topic,
                args,
                None,
            )
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            args = {
                "topic_arn": self._get_topic_arn(op_parser),
                "endpoint": self._get_subscription_endpoint(op_parser),
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
                "topic_arn": self._get_topic_arn(op_parser),
                "endpoint": self._get_subscription_endpoint(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                client_helper.get_subscription,
                args,
                None,
            )
        # PUT
        elif op_parser.op_equals(MessagingOperation.PUT):
            args = op_converter.convert_put(
                self._get_topic_arn(op_parser),
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            call = NCall(
                self._sns_client.publish,
                args,
                nargs,
            )
        # BATCH
        elif op_parser.op_equals(MessagingOperation.BATCH):
            args = {
                "batch": op_parser.get_batch(),
                "topic_arn": self._get_topic_arn(op_parser),
            }
            call = NCall(
                client_helper.batch,
                args,
            )
        # PULL
        elif op_parser.op_equals(MessagingOperation.PULL):
            args = op_converter.convert_pull(
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
            args = {
                "topic": self._get_topic_name(op_parser),
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
        # CREATE TOPIC
        if op_parser.op_equals(MessagingOperation.CREATE_TOPIC):
            result = None
        # UPDATE TOPIC
        elif op_parser.op_equals(MessagingOperation.UPDATE_TOPIC):
            result = None
        # DROP TOPIC
        elif op_parser.op_equals(MessagingOperation.DROP_TOPIC):
            result = None
        # LIST TOPICS
        if op_parser.op_equals(MessagingOperation.LIST_TOPICS):
            result = result_converter.convert_list_topics(nresult)
        # HAS TOPIC
        elif op_parser.op_equals(MessagingOperation.HAS_TOPIC):
            result = nresult
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
            result = result_converter.convert_list_subscriptions(nresult)
        # HAS SUBSCRIPTION
        elif op_parser.op_equals(MessagingOperation.HAS_SUBSCRIPTION):
            result = nresult
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


class ClientHelper:
    sns_client: Any
    sqs_client: Any
    op_converter: OperationConverter

    def __init__(
        self,
        sns_client: Any,
        sqs_client: Any,
        op_converter: OperationConverter,
    ):
        self.sns_client = sns_client
        self.sqs_client = sqs_client
        self.op_converter = op_converter

    def create_topic(
        self,
        topic_name: str,
        config: TopicInfo | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> Any:
        if where_exists is False:
            response = self.sns_client.list_topics()
            for topic in response["Topics"]:
                if topic["TopicArn"].split(":")[-1] == topic_name:
                    raise ConflictError

        args = self.op_converter.convert_create_topic(
            topic_name=topic_name, config=config
        )
        NCall(
            self.sns_client.create_topic,
            args,
            nargs,
        ).invoke()

    def update_topic(
        self,
        topic_arn: str,
        config: TopicConfig | None,
        nargs: Any,
    ) -> None:
        if config is None:
            return
        args = {
            "TopicArn": topic_arn,
        }
        if config.nconfig:
            for key, value in config.nconfig.items():
                args["AttributeName"] = key
                args["AttributeValue"] = value
                NCall(
                    self.sns_client.set_topic_attributes,
                    args,
                    nargs,
                ).invoke()

    def drop_topic(
        self,
        topic_arn: str,
        where_exists: bool | None,
        nargs: Any,
    ) -> None:
        if where_exists is True:
            try:
                self.sns_client.get_topic_attributes(TopicArn=topic_arn)
            except ClientError as e:
                if e.response["Error"]["Code"] == "NotFound":
                    raise NotFoundError
                else:
                    raise
        NCall(
            self.sns_client.delete_topic,
            {"TopicArn": topic_arn},
            nargs,
        ).invoke()

    def has_topic(
        self,
        topic_arn: str,
        nargs: Any,
    ) -> bool:
        try:
            self.sns_client.get_topic_attributes(TopicArn=topic_arn)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotFound":
                return False
            else:
                raise
        return True

    def get_topic(
        self,
        topic_arn: str,
        nargs: Any,
    ) -> TopicInfo:
        try:
            response = NCall(
                self.sns_client.get_topic_attributes,
                {"TopicArn": topic_arn},
                nargs,
            ).invoke()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotFound":
                raise NotFoundError
            else:
                raise
        return TopicInfo(
            name=topic_arn.split(":")[-1],
            subscription_count=int(
                response["Attributes"].get("SubscriptionsConfirmed", 0)
            ),
            nconfig=response["Attributes"],
            nref=topic_arn,
        )

    def create_subscription(
        self,
        topic_arn: str,
        protocol: str,
        endpoint: str,
        config: SubscriptionInfo | None,
        where_exists: bool | None,
        create_drop_queue: bool,
        nargs: Any,
    ) -> Any:
        if protocol == "sqs" and endpoint.startswith("arn:aws:sqs:"):
            queue_name = endpoint.split(":")[-1]
            try:
                self.sqs_client.get_queue_url(QueueName=queue_name)
            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "AWS.SimpleQueueService.NonExistentQueue"
                ):
                    if not create_drop_queue:
                        raise
                    queue_attributes = (
                        self.op_converter.convert_queue_attributes(
                            topic_arn=topic_arn,
                            endpoint=endpoint,
                            config=config,
                        )
                    )
                    self.sqs_client.create_queue(
                        QueueName=queue_name, Attributes=queue_attributes
                    )
        if where_exists is False:
            response = self.sns_client.list_subscriptions_by_topic(
                TopicArn=topic_arn
            )
            for subscription in response["Subscriptions"]:
                if subscription["Endpoint"] == endpoint:
                    raise ConflictError

        args = self.op_converter.convert_create_subscription(
            topic_arn=topic_arn,
            protocol=protocol,
            endpoint=endpoint,
            config=config,
        )
        return NCall(
            self.sns_client.subscribe,
            args,
            nargs,
        ).invoke()

    def update_subscription(
        self,
        topic_arn: str,
        endpoint: str,
        queue_url: str,
        config: SubscriptionConfig | None,
        nargs: Any,
    ) -> None:
        attributes = self.op_converter.convert_queue_attributes(
            topic_arn=topic_arn, endpoint=endpoint, config=config
        )
        NCall(
            self.sqs_client.set_queue_attributes,
            {
                "QueueUrl": queue_url,
                "Attributes": attributes,
            },
            nargs,
        ).invoke()

    def drop_subscription(
        self,
        topic_arn: str,
        endpoint: str,
        queue_url: str,
        where_exists: bool | None,
        create_drop_queue: bool,
        nargs: Any,
    ) -> None:
        response = self.sns_client.list_subscriptions_by_topic(
            TopicArn=topic_arn
        )
        subscription_arn = None
        for subscription in response["Subscriptions"]:
            if subscription["Endpoint"] == endpoint:
                subscription_arn = subscription["SubscriptionArn"]
        if not subscription_arn:
            if where_exists is True:
                raise NotFoundError
            return

        if create_drop_queue:
            subscription_details = self.sns_client.get_subscription_attributes(
                SubscriptionArn=subscription_arn
            )
        self.sns_client.unsubscribe(SubscriptionArn=subscription_arn)
        if create_drop_queue:
            if (
                "Attributes" in subscription_details
                and "Protocol" in subscription_details["Attributes"]
                and subscription_details["Attributes"]["Protocol"] == "sqs"
                and "Endpoint" in subscription_details["Attributes"]
            ):
                endpoint = subscription_details["Attributes"]["Endpoint"]
                if endpoint.startswith("arn:aws:sqs:"):
                    self.sqs_client.delete_queue(QueueUrl=queue_url)

    def has_subscription(
        self,
        topic_arn: str,
        endpoint: str,
        nargs: Any,
    ) -> bool:
        response = self.sns_client.list_subscriptions_by_topic(
            TopicArn=topic_arn
        )
        for subscription in response["Subscriptions"]:
            if subscription["Endpoint"] == endpoint:
                return True
        return False

    def get_subscription(
        self,
        topic_arn: str,
        endpoint: str,
        nargs: Any,
    ) -> SubscriptionInfo:
        try:
            queue_name = endpoint.split(":")[-1]
            res = self.sqs_client.get_queue_url(QueueName=queue_name)
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
            return SubscriptionInfo(
                name=endpoint.split(":")[-1],
                topic=topic_arn.split(":")[-1],
                active_message_count=int(
                    attributes.get("ApproximateNumberOfMessages", 0)
                ),
                inflight_message_count=int(
                    attributes.get("ApproximateNumberOfMessagesNotVisible", 0)
                ),
                scheduled_message_count=int(
                    attributes.get("ApproximateNumberOfMessagesDelayed", 0)
                ),
                config=SubscriptionConfig(
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
                raise NotFoundError
            raise

    def batch(
        self,
        batch: MessageBatch,
        topic_arn: str,
    ) -> list:
        entries = []
        for i, operation in enumerate(batch.operations):
            op_parser = MessagingOperationParser(operation)
            args = self.op_converter.convert_put(
                topic_arn,
                op_parser.get_value(),
                op_parser.get_metadata(),
                op_parser.get_properties(),
                op_parser.get_put_config(),
            )
            entry = {
                "Id": str(i),
                "Message": args["Message"],
            }

            if "MessageAttributes" in args:
                entry["MessageAttributes"] = args["MessageAttributes"]

            if "MessageGroupId" in args:
                entry["MessageGroupId"] = args["MessageGroupId"]

            if "MessageDeduplicationId" in args:
                entry["MessageDeduplicationId"] = args[
                    "MessageDeduplicationId"
                ]

            entries.append(entry)
        response = NCall(
            self.sns_client.publish_batch,
            {"TopicArn": topic_arn, "PublishBatchRequestEntries": entries},
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

    def close(self, topic: str | None, nargs: Any) -> Any:
        return None


class OperationConverter:
    def convert_create_topic(
        self,
        topic_name: str,
        config: TopicConfig | None,
    ) -> dict:
        args = {
            "Name": topic_name,
        }
        if config and config.nconfig:
            args["Attributes"] = config.nconfig
        return args

    def convert_queue_attributes(
        self,
        topic_arn: str,
        endpoint: str,
        config: SubscriptionConfig | None,
    ) -> dict:
        attributes = {}
        attributes["Policy"] = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "sns.amazonaws.com"},
                        "Action": "sqs:SendMessage",
                        "Resource": endpoint,
                        "Condition": {
                            "ArnEquals": {"aws:SourceArn": topic_arn}
                        },
                    }
                ],
            }
        )
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

    def convert_create_subscription(
        self,
        topic_arn: str,
        protocol: str,
        endpoint: str,
        config: SubscriptionConfig | None,
    ) -> dict:
        args = {
            "TopicArn": topic_arn,
            "Protocol": protocol,
            "Endpoint": endpoint,
        }
        return args

    def convert_put(
        self,
        topic_arn: str,
        value: MessageValueType,
        metadata: dict | None,
        properties: MessageProperties | None,
        config: MessagePutConfig | None,
    ) -> dict:
        if config is not None and config.delay is not None:
            raise BadRequestError("Delay is not supported for SNS publish")

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
            "TopicArn": topic_arn,
            "Message": message_body,
        }

        if message_attributes:
            args["MessageAttributes"] = message_attributes

        if message_group_id and topic_arn.endswith(".fifo"):
            args["MessageGroupId"] = message_group_id

        if message_deduplication_id and topic_arn.endswith(".fifo"):
            args["MessageDeduplicationId"] = message_deduplication_id

        return args

    def convert_pull(
        self, queue_url: str, config: MessagePullConfig | None
    ) -> dict:
        max_messages = 1
        if config is not None and config.max_count is not None:
            max_messages = min(config.max_count, 10)  # SQS allows max 10

        args = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": max_messages,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }

        if config is not None and config.max_wait_time is not None:
            args["WaitTimeSeconds"] = min(
                int(config.max_wait_time), 20
            )  # SQS allows max 20
        if config is not None and config.visibility_timeout is not None:
            args["VisibilityTimeout"] = int(config.visibility_timeout)

        return args


class ResultConverter:
    def _convert_key(self, message: dict) -> MessageKey:
        return MessageKey(nref=message.get("ReceiptHandle"))

    def _convert_value(self, message: dict) -> MessageValueType:
        message_attributes = message.get("MessageAttributes", {})
        content_type = message_attributes.get(
            "content_type", {"Value": "text/plain"}
        ).get("Value", "text/plain")

        body = message.get("Message", "")

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
            # Skip internal attributes
            if key in ["content_type", "message_id", "group_id"]:
                continue

            if attr.get("Type") == "String":
                metadata[key] = attr.get("Value")
            elif attr.get("Type") == "Number":
                try:
                    metadata[key] = float(attr.get("Value"))
                    if metadata[key].is_integer():
                        metadata[key] = int(metadata[key])
                except (ValueError, TypeError):
                    metadata[key] = attr.get("Value")
            elif attr.get("Type") == "Binary":
                metadata[key] = attr.get("Value")
            else:
                metadata[key] = attr.get("Value")

        return metadata if metadata else None

    def _convert_properties(
        self, message: dict, attributes: dict
    ) -> MessageProperties:
        message_attributes = message.get("MessageAttributes", {})
        message_id = None
        if "message_id" in message_attributes:
            message_id = message_attributes["message_id"].get("Value")

        group_id = None
        if "group_id" in message_attributes:
            group_id = message_attributes["group_id"].get("Value")

        content_type = "text/plain"
        if "content_type" in message_attributes:
            content_type = message_attributes["content_type"].get(
                "Value", "text/plain"
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
            delivery_count=(int(attributes.get("ApproximateReceiveCount", 1))),
        )

    def _convert_message(
        self,
        message: dict,
    ) -> MessageItem:
        message_body = json.loads(message.get("Body", "{}"))
        attributes = message.get("Attributes", {})
        return MessageItem(
            key=self._convert_key(message=message),
            value=self._convert_value(message=message_body),
            metadata=self._convert_metadata(message=message_body),
            properties=self._convert_properties(
                message=message_body, attributes=attributes
            ),
        )

    def convert_list_topics(self, nresult: Any) -> list[str]:
        topics = []
        for topic in nresult.get("Topics", []):
            topic_name = topic["TopicArn"].split(":")[-1]
            topics.append(topic_name)
        return topics

    def convert_list_subscriptions(self, nresult: Any) -> list[str]:
        subscriptions = []
        for subscription in nresult.get("Subscriptions", []):
            subscriptions.append(subscription["Endpoint"].split(":")[-1])
        return subscriptions

    def convert_pull(self, nresult: Any) -> list[MessageItem]:
        result = []
        for message in nresult.get("Messages", []):
            result.append(self._convert_message(message))
        return result
