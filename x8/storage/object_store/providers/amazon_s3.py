"""
Object Store on Amazon S3.
"""

from __future__ import annotations

__all__ = ["AmazonS3"]

import json
from datetime import datetime, timezone
from io import BytesIO
from typing import IO, Any

import boto3
import s3transfer.tasks
import s3transfer.upload
from boto3.s3.transfer import TransferConfig as S3TransferConfig
from botocore.exceptions import ClientError
from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    NotModified,
    PreconditionFailedError,
)
from x8.storage._common import (
    CollectionResult,
    CollectionStatus,
    ItemProcessor,
    MatchCondition,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)

from .._helper import (
    QueryArgs,
    get_collection_config,
    get_query_args,
    get_transfer_config,
)
from .._models import (
    ObjectCollectionConfig,
    ObjectItem,
    ObjectKey,
    ObjectList,
    ObjectProperties,
    ObjectStoreClass,
    ObjectTransferConfig,
    ObjectVersion,
)


# Monkey patching to return Etag and Version from upload* methods.
# https://github.com/boto/s3transfer/issues/82#issuecomment-837971614
# -----------------------------
class PutObjectTask(s3transfer.tasks.Task):
    # Copied from s3transfer/upload.py,
    # changed to return the result of client.put_object.
    def _main(self, client, fileobj, bucket, key, extra_args):
        with fileobj as body:
            return client.put_object(
                Bucket=bucket, Key=key, Body=body, **extra_args
            )


class CompleteMultipartUploadTask(s3transfer.tasks.Task):
    # Copied from s3transfer/tasks.py, changed to return a result.
    def _main(self, client, bucket, key, upload_id, parts, extra_args):
        return client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
            **extra_args,
        )


s3transfer.upload.PutObjectTask = PutObjectTask
s3transfer.upload.CompleteMultipartUploadTask = CompleteMultipartUploadTask
# -----------------------------


class AmazonS3(StoreProvider):
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    bucket: str | None
    nparams: dict[str, Any]

    _resource: Any

    _collection_cache: dict[str, AmazonS3Collection]

    def __init__(
        self,
        region: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        bucket: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            region:
                AWS region name.
            profile_name:
                AWS profile name.
            aws_access_key_id:
                AWS access key id.
            aws_secret_access_key:
                AWS secret access key.
            aws_session_token:
                AWS session token.
            bucket:
                S3 bucket mapped to object store collection.
            nparams:
                Native parameters to boto3 client.
        """
        self.region = region
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.bucket = bucket
        self.nparams = nparams

        self._resource = None
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._resource is not None:
            return

        resource = None
        if self.profile_name is not None:
            resource = boto3.resource(
                "s3",
                region_name=self.region,
                profile_name=self.profile_name,
                **self.nparams,
            )
        elif (
            self.aws_access_key_id is not None
            and self.aws_secret_access_key is not None
        ):
            resource = boto3.resource(
                "s3",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                **self.nparams,
            )
        else:
            resource = boto3.resource(
                "s3",
                region_name=self.region,
                **self.nparams,
            )

        self._resource = resource

    def _get_bucket_name(self, op_parser: StoreOperationParser) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        bucket = (
            collection_name if collection_name is not None else self.bucket
        )
        return bucket

    def _get_collection(
        self, op_parser: StoreOperationParser
    ) -> AmazonS3Collection | None:
        if op_parser.is_resource_op():
            return None
        bucket_name = self._get_bucket_name(op_parser)
        if bucket_name is None:
            raise BadRequestError("Collection name must be specified")
        if bucket_name in self._collection_cache:
            return self._collection_cache[bucket_name]
        client = self._resource.meta.client
        col = AmazonS3Collection(
            self._resource,
            client,
            bucket_name,
        )
        self._collection_cache[bucket_name] = col
        return col

    def _validate(self, op_parser: StoreOperationParser):
        pass

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        collection = self._get_collection(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collection,
            ResourceHelper(self._resource),
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult, nerror = ncall.invoke(return_error=True)
        result = self._convert_nresult(
            nresult, nerror, state, op_parser, collection
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: AmazonS3Collection | None,
        resource_helper: Any,
    ) -> tuple[NCall | None, dict]:
        if collection is not None:
            op_converter = collection.op_converter
            client = collection.client
            helper = collection.helper
        call = None
        state: dict = {}
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            args = {
                "bucket": self._get_bucket_name(op_parser),
                "config": get_collection_config(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.create_collection, args)
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            args = {
                "bucket": self._get_bucket_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.drop_collection, args)
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_collections, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = {
                "bucket": self._get_bucket_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.has_collection, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, func, state = op_converter.convert_put(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value_as_bytes(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
                config=get_transfer_config(op_parser),
            )
            if func == "put_object":
                call = NCall(client.put_object, args, nargs)
            elif func == "upload_fileobj":
                call = NCall(client.upload_fileobj, args, nargs)
            elif func == "upload_file" or func == "put_object_exists":
                call = NCall(
                    helper.put,
                    {
                        "id": op_parser.get_id_as_str(),
                        "args": args,
                        "returning": op_parser.get_returning_as_bool(),
                        "func": func,
                        "nargs": nargs,
                    },
                )
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            args, func, state = op_converter.convert_get(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                match_condition=op_parser.get_match_condition(),
                start=op_parser.get_start(),
                end=op_parser.get_end(),
                config=get_transfer_config(op_parser),
            )
            if func == "get_object":
                call = NCall(client.get_object, args, nargs)
            elif func == "download_fileobj":
                call = NCall(client.download_fileobj, args, nargs)
            elif func == "download_file":
                call = NCall(client.download_file, args, nargs)
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            args = op_converter.convert_get_properties(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=op_parser.get_match_condition(),
            )
            call = NCall(client.head_object, args, nargs)
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            args = op_converter.convert_get_versions(
                id=op_parser.get_id_as_str()
            )
            call = NCall(client.list_object_versions, args, nargs)
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args = op_converter.convert_update(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
            )
            call = NCall(client.copy_object, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = {
                "id": op_parser.get_id_as_str(),
                "version": op_parser.get_version(),
                "match_condition": op_parser.get_match_condition(),
                "nargs": nargs,
            }
            call = NCall(helper.delete, args, None)
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            args = op_converter.convert_copy(
                id=op_parser.get_id_as_str(),
                source_id=op_parser.get_source_id_as_str(),
                source_version=op_parser.get_source_version(),
                source_collection=op_parser.get_source_collection(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
            )
            call = NCall(client.copy_object, args, nargs)
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            args = op_converter.convert_generate(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                method=op_parser.get_method(),
                expiry=op_parser.get_expiry_in_seconds(),
            )
            call = NCall(client.generate_presigned_url, args, nargs)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            query_args = get_query_args(op_parser)
            state["query_args"] = query_args
            args = {"query_args": get_query_args(op_parser), "nargs": nargs}
            call = NCall(helper.query, args, None)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            query_args = get_query_args(op_parser)
            state["query_args"] = query_args
            args = {"query_args": get_query_args(op_parser), "nargs": nargs}
            call = NCall(helper.query, args, None)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, func = op_converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            if func == "delete":
                call = NCall(client.delete_objects, args, nargs)
            state["func"] = func
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(resource_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        nerror: Any,
        state: dict,
        op_parser: StoreOperationParser,
        collection: AmazonS3Collection | None,
    ) -> Any:
        if collection is not None:
            result_converter = collection.result_converter
        result: Any = None

        def check_error(nerror):
            if nerror is not None:
                if isinstance(nerror, ClientError):
                    if (
                        nerror.response["Error"]["Code"] == "NoSuchKey"
                        or nerror.response["Error"]["Code"] == "404"
                    ):
                        raise NotFoundError
                    elif (
                        nerror.response["Error"]["Code"]
                        == "PreconditionFailed"
                        or nerror.response["Error"]["Code"] == "412"
                    ):
                        raise PreconditionFailedError
                    elif nerror.response["Error"]["Code"] == "304":
                        raise NotModified
                raise nerror

        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            exists = op_parser.get_where_exists()
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ResourceInUseException"
                ) or (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "BucketAlreadyOwnedByYou"
                ):
                    if exists is False:
                        raise ConflictError
                    else:
                        result = CollectionResult(
                            status=CollectionStatus.EXISTS
                        )
                else:
                    raise nerror
            else:
                result = CollectionResult(status=CollectionStatus.CREATED)
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            exists = op_parser.get_where_exists()
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"] == "NoSuchBucket"
                ):
                    if exists is True:
                        raise NotFoundError
                    else:
                        result = CollectionResult(
                            status=CollectionStatus.NOT_EXISTS
                        )
                else:
                    raise nerror
            else:
                result = CollectionResult(status=CollectionStatus.DROPPED)
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            if nerror is not None:
                raise nerror
            result = [b.name for b in nresult]
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"] == "404"
                ):
                    result = False
                else:
                    raise nerror
            else:
                result = True
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            check_error(nerror)
            if "open_file" in state:
                state["open_file"].close()
            result = result_converter.convert_put_result(
                op_parser.get_id_as_str(),
                nresult,
                op_parser.get_value_as_bytes(),
                op_parser.get_returning(),
            )
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            check_error(nerror)
            result = result_converter.convert_get_result(
                op_parser.get_id_as_str(),
                op_parser.get_file(),
                op_parser.get_stream(),
                nresult,
                state,
            )
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            check_error(nerror)
            result = result_converter.convert_get_properties_result(
                op_parser.get_id_as_str(), nresult
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            check_error(nerror)
            result = result_converter.convert_get_versions_result(
                op_parser.get_id_as_str(), nresult
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            check_error(nerror)
            result = result_converter.convert_copy_result(
                op_parser.get_id_as_str(), nresult
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            check_error(nerror)
            result = None
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            check_error(nerror)
            result = result_converter.convert_copy_result(
                op_parser.get_id_as_str(), nresult
            )
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            check_error(nerror)
            result = result_converter.convert_generate_result(
                op_parser.get_id_as_str(),
                op_parser.get_version(),
                nresult,
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            if nerror is not None:
                raise nerror
            result = result_converter.convert_query_result(
                nresult,
                state["query_args"],
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            if nerror is not None:
                raise nerror
            result = result_converter.convert_count_result(
                nresult,
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = []
            func = state["func"]
            if func == "delete":
                for op_parser in op_parser.get_operation_parsers():
                    result.append(None)
        return result


class ResourceHelper:
    resource: Any

    def __init__(self, resource: Any):
        self.resource = resource

    def create_collection(
        self, bucket: str, config: ObjectCollectionConfig | None, nargs: Any
    ):
        args: dict = {
            "Bucket": bucket,
        }
        if config is not None and config.nconfig is not None:
            args = args | config.nconfig
        if "CreateBucketConfiguration" not in args:
            region_name = self.resource.meta.client.meta.region_name
            args["CreateBucketConfiguration"] = {
                "LocationConstraint": region_name
            }
        s3_bucket = NCall(
            self.resource.create_bucket,
            args,
            nargs,
        ).invoke()
        if config is not None and config.acl is not None:
            if config.acl == "public-read":
                NCall(
                    self.resource.meta.client.put_public_access_block,
                    {
                        "Bucket": bucket,
                        "PublicAccessBlockConfiguration": {
                            "BlockPublicPolicy": False,
                        },
                    },
                    None,
                ).invoke()
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "PublicReadGetObject",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": ["s3:GetObject"],
                            "Resource": [f"arn:aws:s3:::{bucket}/*"],
                        }
                    ],
                }
                NCall(
                    self.resource.meta.client.put_bucket_policy,
                    {"Bucket": bucket, "Policy": json.dumps(policy)},
                    None,
                ).invoke()
        if config is not None and config.versioned:
            NCall(
                self.resource.meta.client.put_bucket_versioning,
                {
                    "Bucket": bucket,
                    "VersioningConfiguration": {"Status": "Enabled"},
                },
                None,
            ).invoke()
        NCall(s3_bucket.wait_until_exists, None, nargs).invoke()

    def drop_collection(self, bucket: str, nargs: Any):
        NCall(
            self.resource.meta.client.delete_bucket, {"Bucket": bucket}, nargs
        ).invoke()
        waiter = NCall(
            self.resource.meta.client.get_waiter, ["bucket_not_exists"], nargs
        ).invoke()
        NCall(waiter.wait, {"Bucket": bucket}, nargs).invoke()

    def list_collections(self, nargs) -> Any:
        response = NCall(self.resource.buckets.all, None, nargs).invoke()
        return response

    def has_collection(self, bucket: str, nargs: Any):
        NCall(
            self.resource.meta.client.head_bucket, {"Bucket": bucket}, nargs
        ).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class ClientHelper:
    resource: Any
    client: Any
    bucket: str

    def __init__(self, resource: Any, client: Any, bucket: str) -> None:
        self.resource = resource
        self.client = client
        self.bucket = bucket

    def put(
        self,
        id: str,
        args: dict,
        returning: bool | None,
        func: str,
        nargs: Any,
    ) -> Any:
        if func == "upload_file":
            NCall(self.client.upload_file, args, nargs).invoke()
            if returning is True:
                args = {"Bucket": self.bucket, "Key": id}
                return NCall(self.client.head_object, args, None).invoke()
        elif func == "put_object_exists":
            try:
                NCall(
                    self.client.head_object,
                    {"Bucket": self.bucket, "Key": id},
                ).invoke()
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise PreconditionFailedError
            return NCall(self.client.put_object, args, nargs).invoke()
        return None

    def delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        nargs: Any,
    ) -> Any:
        if version == "*":
            return NCall(
                self.resource.Bucket(self.bucket)
                .object_versions.filter(Prefix=id)
                .delete,
                None,
                nargs,
            ).invoke()
        else:
            args = {"Bucket": self.bucket, "Key": id}
            if version is not None:
                args["VersionId"] = version
            try:
                response = NCall(
                    self.client.head_object,
                    args,
                ).invoke()
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    if match_condition.if_match or match_condition.exists:
                        raise PreconditionFailedError
                    raise NotFoundError
            if match_condition.if_match:
                etag = response["ETag"]
                if (
                    match_condition.if_match != "*"
                    and match_condition.if_match != etag
                ):
                    raise PreconditionFailedError
            return NCall(
                self.client.delete_object,
                args,
            ).invoke()
        return None

    def query(
        self,
        query_args: QueryArgs,
        nargs: Any,
    ) -> Any:
        responses: list = []
        args: dict = {}
        args["Bucket"] = self.bucket
        if query_args.prefix is not None:
            args["Prefix"] = query_args.prefix
        if query_args.delimiter is not None:
            args["Delimiter"] = query_args.delimiter
        if query_args.start_after is not None:
            args["StartsAfter"] = query_args.start_after
        continuation_token = query_args.continuation
        total_key_count = 0
        while True:
            if continuation_token is not None:
                args["ContinuationToken"] = continuation_token
            if (
                query_args.limit is not None
                and query_args.page_size is not None
            ):
                if query_args.page_size < query_args.limit - total_key_count:
                    args["MaxKeys"] = query_args.page_size
                else:
                    args["MaxKeys"] = query_args.limit - total_key_count
            elif query_args.limit is not None:
                args["MaxKeys"] = query_args.limit - total_key_count
            elif query_args.page_size is not None:
                args["MaxKeys"] = query_args.page_size
            response = NCall(self.client.list_objects_v2, args, nargs).invoke()
            responses.append(response)
            key_count = response["KeyCount"]
            total_key_count += key_count
            if (
                query_args.limit is not None
                and total_key_count >= query_args.limit
            ):
                break
            if "NextContinuationToken" in response:
                continuation_token = response["NextContinuationToken"]
            else:
                break
            if query_args.paging:
                break
        return responses


class ResultConverter:
    processor: ItemProcessor
    bucket: str

    def __init__(self, processor: ItemProcessor, bucket: str):
        self.processor = processor
        self.bucket = bucket

    def convert_url(self, id: str):
        return f"https://{self.bucket}.s3.amazonaws.com/{id}"

    def convert_put_result(
        self,
        id: str,
        nresult: Any,
        value: bytes | None,
        returning: str | None,
    ) -> ObjectItem:
        version = None
        etag = None
        if nresult is not None and isinstance(nresult, dict):
            if "VersionId" in nresult:
                version = nresult["VersionId"]
            if "ETag" in nresult:
                etag = nresult["ETag"]
        return_value = None
        if returning == "new":
            return_value = value
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            value=return_value,
            properties=ObjectProperties(etag=etag),
            url=self.convert_url(id=id),
        )

    def convert_get_result(
        self,
        id: str,
        file: str | None,
        stream: IO | None,
        nresult: Any,
        state: dict,
    ) -> ObjectItem:
        value = None
        version = None
        metadata = None
        func = state["func"]
        if func == "get_object":
            body = nresult["Body"]
            if file is not None:
                with open(file, "wb") as f:
                    f.write(body.read())
            elif stream is not None:
                stream.write(body.read())
            else:
                value = body.read()
            if "VersionId" in nresult:
                version = nresult["VersionId"]
            if "Metadata" in nresult:
                metadata = nresult["Metadata"]
            properties = ResultConverter.convert_properties(nresult)
            return ObjectItem(
                key=ObjectKey(id=id, version=version),
                value=value,
                metadata=metadata,
                properties=properties,
                url=self.convert_url(id=id),
            )
        elif func == "download_file":
            return ObjectItem(
                key=ObjectKey(id=id), url=self.convert_url(id=id)
            )
        else:
            if stream is None:
                fileobj = state["fileobj"]
                fileobj.seek(0)
                value = fileobj.read()
                return ObjectItem(
                    key=ObjectKey(id=id),
                    value=value,
                    url=self.convert_url(id=id),
                )
            return ObjectItem(
                key=ObjectKey(id=id), url=self.convert_url(id=id)
            )

    def convert_get_properties_result(
        self, id: str, nresult: Any
    ) -> ObjectItem:
        version = None
        metadata = None
        properties = ObjectProperties()
        if "VersionId" in nresult:
            version = nresult["VersionId"]
        key = ObjectKey(id=id, version=version)
        if "Metadata" in nresult:
            metadata = nresult["Metadata"]
        properties = ResultConverter.convert_properties(nresult)
        return ObjectItem(
            key=key,
            metadata=metadata,
            properties=properties,
            url=self.convert_url(id=id),
        )

    def convert_get_versions_result(self, id: str, nresult: Any) -> ObjectItem:
        key = ObjectKey(id=id)
        versions: list = []
        if "Versions" in nresult:
            for version in nresult["Versions"]:
                if version["Key"] != id:
                    continue
                properties = ResultConverter.convert_properties(version)
                versions.append(
                    ObjectVersion(
                        version=version["VersionId"],
                        properties=properties,
                        latest=version["IsLatest"],
                    )
                )
        if len(versions) == 0:
            raise NotFoundError
        return ObjectItem(
            key=key, versions=versions[::-1], url=self.convert_url(id=id)
        )

    def convert_copy_result(self, id: str, nresult: Any) -> ObjectItem:
        version = None
        etag = None
        last_modified = None
        if "VersionId" in nresult:
            version = nresult["VersionId"]
        if "CopyObjectResult" in nresult:
            etag = nresult["CopyObjectResult"]["ETag"]
            last_modified = nresult["CopyObjectResult"][
                "LastModified"
            ].timestamp()
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            properties=ObjectProperties(
                etag=etag, last_modified=last_modified
            ),
            url=self.convert_url(id=id),
        )

    def convert_generate_result(
        self, id: str, version: str | None, nresult: Any
    ) -> ObjectItem:
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            url=nresult,
        )

    def convert_query_result(
        self, nresult: Any, query_args: QueryArgs
    ) -> ObjectList:
        items: list = []
        prefixes: list = []
        total_count = 0
        continuation = None
        for response in nresult:
            total_count += response["KeyCount"]
            if "Contents" in response:
                for content in response["Contents"]:
                    item = ObjectItem(
                        key=ObjectKey(id=content["Key"]),
                        properties=ResultConverter.convert_properties(content),
                        url=self.convert_url(id=content["Key"]),
                    )
                    items.append(item)
            if "CommonPrefixes" in response:
                for prefix in response["CommonPrefixes"]:
                    prefixes.append(prefix["Prefix"])
            if "NextContinuationToken" in response:
                continuation = response["NextContinuationToken"]
            else:
                continuation = None
        if not query_args.paging:
            continuation = None
        if query_args.limit is not None:
            if total_count >= query_args.limit:
                continuation = None
        return ObjectList(
            items=items,
            continuation=continuation,
            prefixes=prefixes,
        )

    def convert_count_result(
        self,
        nresult: Any,
    ) -> int:
        total_count = 0
        for response in nresult:
            total_count += response["KeyCount"]
        return total_count

    @staticmethod
    def convert_storage_class(storage_class: str | None) -> str:
        map = {
            "STANDARD": ObjectStoreClass.HOT,
            "STANDARD_IA": ObjectStoreClass.COOL,
            "GLACIER": ObjectStoreClass.COLD,
            "DEEP_ARCHIVE": ObjectStoreClass.ARCHIVE,
        }
        if storage_class is None:
            return ObjectStoreClass.HOT.value
        if storage_class in map:
            return map[storage_class].value
        return storage_class

    @staticmethod
    def convert_properties(nresult: Any) -> ObjectProperties:
        properties = ObjectProperties()
        if "CacheControl" in nresult:
            properties.cache_control = nresult["CacheControl"]
        if "ContentDisposition" in nresult:
            properties.content_disposition = nresult["ContentDisposition"]
        if "ContentEncoding" in nresult:
            properties.content_encoding = nresult["ContentEncoding"]
        if "ContentLanguage" in nresult:
            properties.content_language = nresult["ContentLanguage"]
        if "ContentLength" in nresult:
            properties.content_length = nresult["ContentLength"]
        if "ContentMD5" in nresult:
            properties.content_md5 = nresult["ContentMD5"]
        if "ContentType" in nresult:
            properties.content_type = nresult["ContentType"]
        if "ChecksumCRC32C" in nresult:
            properties.crc32c = nresult["ChecksumCRC32C"]
        if "LastModified" in nresult:
            properties.last_modified = nresult["LastModified"].timestamp()
        if "ETag" in nresult:
            properties.etag = nresult["ETag"]
        if "Size" in nresult:
            properties.content_length = nresult["Size"]
        if "Expires" in nresult:
            properties.expires = nresult["Expires"].timestamp()
        if "StorageClass" in nresult:
            properties.storage_class = ResultConverter.convert_storage_class(
                nresult["StorageClass"]
            )
        else:
            properties.storage_class = ObjectStoreClass.HOT
        return properties


class OperationConverter:
    processor: ItemProcessor
    bucket: str

    def __init__(self, processor: ItemProcessor, bucket: str):
        self.processor = processor
        self.bucket = bucket

    def convert_transfer_config(
        self, config: ObjectTransferConfig | None
    ) -> S3TransferConfig:
        if config is None:
            return None
        args: dict = {}
        if config is not None:
            if config.multipart is not None:
                args["multipart_threshold"] = 1
            if config.concurrency is not None:
                args["max_concurrency"] = config.concurrency
            if config.chunksize is not None:
                args["multipart_chunksize"] = config.chunksize
            if config.nconfig is not None:
                args = args | config.nconfig
        return S3TransferConfig(**args)

    def convert_put(
        self,
        id: str,
        value: bytes | None,
        file: str | None,
        stream: IO | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        config: ObjectTransferConfig | None,
    ) -> tuple[dict, str, dict]:
        args: dict = {}
        state: dict = {}
        func = "put_object"
        args["Bucket"] = self.bucket
        args["Key"] = id
        if config is not None and config.multipart:
            if value is not None:
                func = "upload_fileobj"
                args["Fileobj"] = BytesIO(value)
            elif stream is not None:
                func = "upload_fileobj"
                args["Fileobj"] = stream
            elif file is not None:
                func = "upload_file"
                args["Filename"] = file
            args["ExtraArgs"] = OperationConverter.convert_properties_metadata(
                properties, metadata
            )
            args["Config"] = self.convert_transfer_config(config)
        else:
            func = "put_object"
            if value is not None:
                args["Body"] = value
            elif stream is not None:
                args["Body"] = stream
            elif file is not None:
                args["Body"] = open(file, "rb")
                state["open_file"] = args["Body"]
            args = args | OperationConverter.convert_properties_metadata(
                properties, metadata
            )
            args = args | OperationConverter.convert_match_condition(
                match_condition
            )
            if match_condition.exists is True:
                func = "put_object_exists"
        return args, func, state

    def convert_get(
        self,
        id: str,
        version: str | None,
        file: str | None,
        stream: IO | None,
        match_condition: MatchCondition,
        start: int | None,
        end: int | None,
        config: ObjectTransferConfig | None,
    ) -> tuple[dict, str, dict]:
        args: dict = {}
        state: dict = {}
        func = "get_object"
        args["Bucket"] = self.bucket
        args["Key"] = id
        if config is not None and config.multipart:
            if file is not None:
                func = "download_file"
                args["Filename"] = file
            elif stream is not None:
                func = "download_fileobj"
                args["Fileobj"] = stream
            else:
                func = "download_fileobj"
                fileobj = BytesIO()
                state["fileobj"] = fileobj
                args["Fileobj"] = fileobj
            if version is not None:
                args["ExtraArgs"] = {"VersionId": version}
            args["Config"] = self.convert_transfer_config(config)
        else:
            func = "get_object"
            if version is not None:
                args["VersionId"] = version
            args = args | OperationConverter.convert_match_condition(
                match_condition
            )
            if start is not None and end is not None:
                args["Range"] = f"bytes={start}-{end}"
            elif start is not None:
                args["Range"] = f"bytes={start}-"
            elif end is not None:
                args["Range"] = f"bytes=0-{end}"
        state["func"] = func
        return args, func, state

    def convert_get_properties(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
    ) -> dict:
        args: dict = {"Bucket": self.bucket, "Key": id}
        if version is not None:
            args["VersionId"] = version
        args = args | OperationConverter.convert_match_condition(
            match_condition
        )
        return args

    def convert_get_versions(self, id: str) -> dict:
        return {"Bucket": self.bucket, "Prefix": id}

    def convert_update(
        self,
        id: str,
        version: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
    ) -> dict:
        args: dict = {}
        args["Bucket"] = self.bucket
        args["Key"] = id
        args["CopySource"] = {"Bucket": self.bucket, "Key": id}
        if version is not None:
            args["CopySource"]["VersionId"] = version
        if metadata is not None:
            args["Metadata"] = metadata
            args["MetadataDirective"] = "REPLACE"
        args = args | OperationConverter.convert_properties_metadata(
            properties, None
        )
        args = args | OperationConverter.convert_copy_match_condition(
            match_condition
        )
        return args

    def convert_copy(
        self,
        id: str,
        source_id: str,
        source_version: str | None,
        source_collection: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
    ):
        args: dict = {}
        args["Bucket"] = self.bucket
        args["Key"] = id
        args["CopySource"] = {
            "Bucket": (
                source_collection
                if source_collection is not None
                else self.bucket
            ),
            "Key": source_id,
        }
        if source_version is not None:
            args["CopySource"]["VersionId"] = source_version
        if metadata is not None:
            args["Metadata"] = metadata
            args["MetadataDirective"] = "REPLACE"
        args = args | OperationConverter.convert_properties_metadata(
            properties, None
        )
        args = args | OperationConverter.convert_copy_match_condition(
            match_condition
        )
        return args

    def convert_generate(
        self,
        id: str,
        version: str | None,
        method: str | None,
        expiry: int | None,
    ):
        args: dict = {}
        args["Params"] = {"Bucket": self.bucket, "Key": id}
        if version is not None:
            args["Params"]["VersionId"] = version
        if method is None or method == "GET":
            args["ClientMethod"] = "get_object"
        elif method == "PUT":
            args["ClientMethod"] = "put_object"
        else:
            raise BadRequestError("Method not supported for generating url")
        if expiry is not None:
            args["ExpiresIn"] = expiry
        return args

    def convert_batch(
        self, op_parsers: list[StoreOperationParser]
    ) -> tuple[dict, str]:
        for op_parser in op_parsers:
            if not op_parser.op_equals(StoreOperation.DELETE):
                raise BadRequestError("Operation not supported in BATCH")
        return self.convert_batch_delete(op_parsers), "delete"

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        args: dict = {}
        args["Bucket"] = self.bucket
        args["Delete"] = {"Objects": []}
        for op_parser in op_parsers:
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            object = {"Key": id}
            if version is not None:
                object["VersionId"] = version
            args["Delete"]["Objects"].append(object)
        return args

    @staticmethod
    def convert_storage_class(storage_class: str | None) -> str:
        map = {
            ObjectStoreClass.HOT: "STANDARD",
            ObjectStoreClass.COOL: "STANDARD_IA",
            ObjectStoreClass.COLD: "GLACIER",
            ObjectStoreClass.ARCHIVE: "DEEP_ARCHIVE",
        }
        if storage_class is None:
            return map[ObjectStoreClass.HOT]
        if ObjectStoreClass(storage_class) in map:
            return map[ObjectStoreClass(storage_class)]
        return storage_class

    @staticmethod
    def convert_properties_metadata(
        properties: dict | None, metadata: dict | None
    ) -> dict:
        args: dict = {}
        if properties is None:
            props = ObjectProperties()
        else:
            props = ObjectProperties(**properties)
        if props.cache_control is not None:
            args["CacheControl"] = props.cache_control
        if props.content_disposition is not None:
            args["ContentDisposition"] = props.content_disposition
        if props.content_encoding is not None:
            args["ContentEncoding"] = props.content_encoding
        if props.content_language is not None:
            args["ContentLanguage"] = props.content_language
        if props.content_length is not None:
            args["ContentLength"] = props.content_length
        if props.content_md5 is not None:
            args["ContentMD5"] = props.content_md5
        if props.content_type is not None:
            args["ContentType"] = props.content_type
        if props.crc32c is not None:
            args["ChecksumCRC32C"] = props.crc32c
        if props.expires is not None:
            args["Expires"] = datetime.fromtimestamp(
                props.expires, tz=timezone.utc
            )
        if props.storage_class is not None:
            args["StorageClass"] = OperationConverter.convert_storage_class(
                props.storage_class
            )
        if metadata is not None:
            args["Metadata"] = metadata
        return args

    @staticmethod
    def convert_match_condition(match_condition: MatchCondition) -> dict:
        args: dict = {}
        # Not supported yet
        # if match_condition.exists is True:
        #    args["IfMatch"] = "*"
        if match_condition.exists is False:
            args["IfNoneMatch"] = "*"
        if match_condition.if_match is not None:
            args["IfMatch"] = match_condition.if_match
        if match_condition.if_none_match is not None:
            args["IfNoneMatch"] = match_condition.if_none_match
        if match_condition.if_modified_since is not None:
            args["IfModifiedSince"] = datetime.fromtimestamp(
                match_condition.if_modified_since, tz=timezone.utc
            )
        if match_condition.if_unmodified_since is not None:
            args["IfUnmodifiedSince"] = datetime.fromtimestamp(
                match_condition.if_unmodified_since, tz=timezone.utc
            )
        return args

    @staticmethod
    def convert_copy_match_condition(match_condition: MatchCondition) -> dict:
        args: dict = {}
        if match_condition.if_match is not None:
            args["CopySourceIfMatch"] = match_condition.if_match
        if match_condition.if_none_match is not None:
            args["CopySourceIfNoneMatch"] = match_condition.if_none_match
        if match_condition.if_modified_since is not None:
            args["CopySourceIfModifiedSince"] = datetime.fromtimestamp(
                match_condition.if_modified_since, tz=timezone.utc
            )
        if match_condition.if_unmodified_since is not None:
            args["CopySourceIfUnmodifiedSince"] = datetime.fromtimestamp(
                match_condition.if_unmodified_since, tz=timezone.utc
            )
        return args


class AmazonS3Collection:
    client: Any
    helper: Any
    op_converter: OperationConverter
    result_converter: ResultConverter
    processor: ItemProcessor

    def __init__(
        self,
        resource: Any,
        client: Any,
        bucket: str,
    ):
        self.client = client
        self.helper = ClientHelper(resource, client, bucket)
        self.processor = ItemProcessor()
        self.op_converter = OperationConverter(self.processor, bucket)
        self.result_converter = ResultConverter(self.processor, bucket)
