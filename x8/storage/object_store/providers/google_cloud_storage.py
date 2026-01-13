"""
Object Store on Google Cloud Storage.
"""

from __future__ import annotations

__all__ = ["GoogleCloudStorage"]

import base64
from datetime import timedelta
from io import BytesIO
from typing import IO, Any

from google.api_core.exceptions import Conflict, NotFound
from google.api_core.exceptions import NotModified as NativeNotModified
from google.api_core.exceptions import PreconditionFailed
from google.cloud.storage import Blob, Bucket, Client

from x8._common.google_provider import GoogleProvider
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


class GoogleCloudStorage(GoogleProvider, StoreProvider):
    project: str | None
    bucket: str | None
    nparams: dict[str, Any]

    _credentials: Any
    _storage_client: Any

    _collection_cache: dict[str, GoogleCloudStorageCollection]

    def __init__(
        self,
        project: str | None = None,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        bucket: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            project:
                Google Cloud project name.
            service_account_info:
                Google service account info with serialized credentials.
            service_account_file:
                Google service account file with credentials.
            access_token:
                Google access token.
            bucket:
                Google Cloud Storage bucket name
                mapped to object store collection.
            nparams:
                Native params to Google Cloud Storage client.
        """
        self.project = project
        self.bucket = bucket
        self.nparams = nparams

        self._storage_client = None
        self._collection_cache = dict()
        GoogleProvider.__init__(
            self,
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._storage_client is not None:
            return

        (
            self._credentials,
            self._storage_client,
        ) = self._init_credentials_client()

    def _init_credentials_client(
        self,
    ) -> tuple[Any, Any]:
        credentials = self._get_credentials()

        args = dict()
        if self.project is not None:
            args["project"] = self.project
        if credentials is not None:
            args["credentials"] = credentials
        if self.nparams is not None:
            args = args | self.nparams
        client: Any = Client(**args)
        return credentials, client

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
    ) -> GoogleCloudStorageCollection | None:
        if op_parser.is_resource_op():
            return None
        bucket_name = self._get_bucket_name(op_parser)
        if bucket_name is None:
            raise BadRequestError("Collection name must be specified")
        if bucket_name in self._collection_cache:
            return self._collection_cache[bucket_name]
        client = self._storage_client.bucket(bucket_name)
        col = GoogleCloudStorageCollection(
            self._storage_client, client, bucket_name
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
            ResourceHelper(self._storage_client),
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(nresult, state, op_parser, collection)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: GoogleCloudStorageCollection | None,
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
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.create_collection,
                args,
            )
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            args = {
                "bucket": self._get_bucket_name(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.drop_collection,
                args,
            )
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
            call = NCall(
                resource_helper.has_collection,
                args,
                None,
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, func, blob = op_converter.convert_put(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value_as_bytes(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
                returning=op_parser.get_returning_as_bool(),
                config=get_transfer_config(op_parser),
            )
            if func == "upload_from_file":
                call = NCall(
                    blob.upload_from_file,
                    args,
                    nargs,
                    {
                        NotFound: PreconditionFailedError,
                        PreconditionFailed: PreconditionFailedError,
                    },
                )
            elif func == "upload_from_filename":
                call = NCall(
                    blob.upload_from_filename,
                    args,
                    nargs,
                    {
                        NotFound: PreconditionFailedError,
                        PreconditionFailed: PreconditionFailedError,
                    },
                )
            elif func == "helper_put":
                args["nargs"] = nargs
                call = NCall(
                    helper.put,
                    args,
                    None,
                    {
                        NotFound: PreconditionFailedError,
                        PreconditionFailed: PreconditionFailedError,
                    },
                )
            state["blob"] = blob
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            args = {
                "id": op_parser.get_id_as_str(),
                "version": op_parser.get_version(),
                "file": op_parser.get_file(),
                "stream": op_parser.get_stream(),
                "match_condition": op_parser.get_match_condition(),
                "start": op_parser.get_start(),
                "end": op_parser.get_end(),
                "config": get_transfer_config(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                helper.get,
                args,
                None,
                {
                    NotFound: NotFoundError,
                    PreconditionFailed: PreconditionFailedError,
                    NativeNotModified: NotModified,
                },
            )
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            args = op_converter.convert_get_properties(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=op_parser.get_match_condition(),
            )
            call = NCall(
                client.get_blob,
                args,
                nargs,
                {
                    NotFound: NotFoundError,
                    None: NotFoundError,
                    PreconditionFailed: PreconditionFailedError,
                    NativeNotModified: NotModified,
                },
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            args = op_converter.convert_get_versions(
                id=op_parser.get_id_as_str()
            )
            call = NCall(
                client.list_blobs,
                args,
                nargs,
                {
                    NotFound: NotFoundError,
                    PreconditionFailed: PreconditionFailedError,
                },
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, blob, update_storage_class, update_others = (
                op_converter.convert_update(
                    id=op_parser.get_id_as_str(),
                    version=op_parser.get_version(),
                    metadata=op_parser.get_metadata(),
                    properties=op_parser.get_properties(),
                    match_condition=op_parser.get_match_condition(),
                )
            )
            call = NCall(
                helper.update,
                {
                    "blob": blob,
                    "args": args,
                    "update_storage_class": update_storage_class,
                    "update_others": update_others,
                    "nargs": nargs,
                },
                nargs,
                {
                    NotFound: NotFoundError,
                    PreconditionFailed: PreconditionFailedError,
                },
            )
            state["blob"] = blob
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args, func = op_converter.convert_delete(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=op_parser.get_match_condition(),
            )
            if func == "helper_delete":
                args["nargs"] = nargs
                call = NCall(
                    helper.delete,
                    args,
                    None,
                    {
                        NotFound: NotFoundError,
                        PreconditionFailed: PreconditionFailedError,
                    },
                )
            elif func == "delete_blob":
                call = NCall(
                    client.delete_blob,
                    args,
                    nargs,
                    {
                        NotFound: NotFoundError,
                        PreconditionFailed: PreconditionFailedError,
                    },
                )
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            args, source_bucket = op_converter.convert_copy(
                id=op_parser.get_id_as_str(),
                source_id=op_parser.get_source_id_as_str(),
                source_version=op_parser.get_source_version(),
                source_collection=op_parser.get_source_collection(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
            )
            call = NCall(
                source_bucket.copy_blob,
                args,
                nargs,
                {
                    NotFound: NotFoundError,
                    PreconditionFailed: PreconditionFailedError,
                },
            )
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            args, blob = op_converter.convert_generate(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                method=op_parser.get_method(),
                expiry=op_parser.get_expiry_in_seconds(),
            )
            call = NCall(blob.generate_signed_url, args, nargs)
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
                args["nargs"] = nargs
                call = NCall(helper.batch_delete, args, None)
            state["func"] = func
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(resource_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict,
        op_parser: StoreOperationParser,
        collection: GoogleCloudStorageCollection | None,
    ) -> Any:
        if collection is not None:
            result_converter = collection.result_converter
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            result = nresult
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            result = nresult
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = [b.name for b in nresult]
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            blob = state["blob"]
            result = result_converter.convert_put_result(
                op_parser.get_id_as_str(),
                blob,
                op_parser.get_value_as_bytes(),
                op_parser.get_returning(),
            )
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            result = result_converter.convert_get_result(
                op_parser.get_id_as_str(),
                nresult,
            )
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            result = result_converter.convert_get_properties_result(
                op_parser.get_id_as_str(), nresult
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            result = result_converter.convert_get_versions_result(
                op_parser.get_id_as_str(), nresult
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            blob = state["blob"]
            result = result_converter.convert_update_result(
                op_parser.get_id_as_str(), blob
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            result = result_converter.convert_copy_result(
                op_parser.get_id_as_str(), nresult
            )
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            result = result_converter.convert_generate_result(
                op_parser.get_id_as_str(),
                op_parser.get_version(),
                nresult,
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            result = result_converter.convert_query_result(
                nresult,
                state["query_args"],
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
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
    storage_client: Any

    def __init__(self, storage_client: Any):
        self.storage_client = storage_client

    def create_collection(
        self,
        bucket: str,
        config: ObjectCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ):
        storage_bucket = self.storage_client.bucket(bucket)
        if config is not None and config.versioned is True:
            storage_bucket.versioning_enabled = True
        args = {"bucket_or_name": storage_bucket}
        if config is not None and config.nconfig is not None:
            args = args | config.nconfig
        try:
            storage_bucket = NCall(
                self.storage_client.create_bucket,
                args,
                nargs,
            ).invoke()
            if config is not None and config.acl is not None:
                if config.acl == "public-read":
                    storage_bucket = NCall(
                        storage_bucket.make_public,
                        {"future": True},
                        None,
                    ).invoke()
        except Conflict:
            if exists is False:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(self, bucket: str, exists: bool | None, nargs: Any):
        gbucket = self.storage_client.bucket(bucket)
        args = {"force": True}
        try:
            NCall(
                gbucket.delete,
                args,
                nargs,
            ).invoke()
        except NotFound:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        response = NCall(
            self.storage_client.list_buckets, None, nargs
        ).invoke()
        return response

    def has_collection(self, bucket: str, nargs: Any):
        gbucket = self.storage_client.bucket(bucket)
        return NCall(
            gbucket.exists,
            None,
            nargs,
        ).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class ClientHelper:
    storage_client: Any
    client: Any
    bucket: str
    op_converter: OperationConverter

    def __init__(
        self,
        storage_client: Any,
        client: Any,
        bucket: str,
        op_converter: OperationConverter,
    ) -> None:
        self.storage_client = storage_client
        self.client = client
        self.bucket = bucket
        self.op_converter = op_converter

    def put(
        self,
        blob: Blob,
        value: bytes | None,
        file: str | None,
        stream: IO | None,
        match_condition: MatchCondition,
        returning: bool | None,
        config: ObjectTransferConfig | None,
        nargs: Any,
    ) -> Any:
        import os
        import tempfile

        from google.cloud.storage import transfer_manager

        raw_value = None
        filename = None
        args: dict = {}
        args["blob"] = blob
        if config is not None:
            if config.chunksize is not None:
                args["chunk_size"] = config.chunksize
            if config.concurrency is not None:
                args["max_workers"] = config.concurrency
        if file is not None:
            filename = file
        elif value is not None:
            raw_value = value
        elif stream is not None:
            raw_value = stream.read()
        if raw_value is not None:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(raw_value)
                filename = temp_file.name
        if filename is None:
            raise BadRequestError("Object not provided")
        args["filename"] = filename
        NCall(
            transfer_manager.upload_chunks_concurrently, args, nargs
        ).invoke()
        if raw_value is not None:
            os.remove(filename)
        if returning is True:
            NCall(blob.reload, None, None).invoke()

    def get(
        self,
        id: str,
        version: str | None,
        file: str | None,
        stream: IO | None,
        match_condition: MatchCondition,
        start: int | None,
        end: int | None,
        config: ObjectTransferConfig | None,
        nargs: Any,
    ) -> Any:
        if version is None:
            blob = self.client.get_blob(blob_name=id)
        else:
            blob = self.client.get_blob(blob_name=id, generation=version)
        if blob is None:
            raise NotFoundError
        args, func = self.op_converter.convert_get(
            id=id,
            version=version,
            file=file,
            stream=stream,
            match_condition=match_condition,
            start=start,
            end=end,
            config=config,
            blob=blob,
        )
        if func == "download_to_filename":
            return (
                NCall(
                    blob.download_to_filename,
                    args,
                    nargs,
                    {
                        NotFound: NotFoundError,
                        PreconditionFailed: PreconditionFailedError,
                        NativeNotModified: NotModified,
                    },
                ).invoke(),
                blob,
            )
        elif func == "download_to_file":
            return (
                NCall(
                    blob.download_to_file,
                    args,
                    nargs,
                    {
                        NotFound: NotFoundError,
                        PreconditionFailed: PreconditionFailedError,
                        NativeNotModified: NotModified,
                    },
                ).invoke(),
                blob,
            )
        elif func == "download_as_bytes":
            return (
                NCall(
                    blob.download_as_bytes,
                    args,
                    nargs,
                    {
                        NotFound: NotFoundError,
                        PreconditionFailed: PreconditionFailedError,
                        NativeNotModified: NotModified,
                    },
                ).invoke(),
                blob,
            )

        import os
        import shutil
        import tempfile

        from google.cloud.storage import transfer_manager

        generated = False
        folder = None
        filename = None
        args = {}
        args["blob"] = blob
        if config is not None:
            if config.chunksize is not None:
                args["chunk_size"] = config.chunksize
            if config.concurrency is not None:
                args["max_workers"] = config.concurrency
        if file is not None:
            filename = file
        else:
            generated = True
            folder = tempfile.mkdtemp()
            filename = os.path.join(folder, "temp")
        args["filename"] = filename
        args["download_kwargs"] = self.op_converter.convert_match_condition(
            match_condition,
            True,
        )
        NCall(
            transfer_manager.download_chunks_concurrently, args, nargs
        ).invoke()
        if generated:
            with open(filename, "rb") as f:
                if stream is not None:
                    stream.write(f.read())
                else:
                    return f.read(), blob
            if folder is not None:
                shutil.rmtree(folder)
        return None, blob

    def update(
        self,
        blob: Blob,
        args: dict,
        update_storage_class: bool,
        update_others: bool,
        nargs: Any,
    ) -> Any:
        result = None
        if update_storage_class:
            new_class = blob.storage_class
            blob.storage_class = None
            args["new_class"] = new_class
            result = NCall(blob.update_storage_class, args, nargs).invoke()
            args.pop("new_class")
        if update_others:
            result = NCall(blob.update, args, nargs).invoke()
        return result

    def delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        nargs: Any,
    ) -> Any:
        if version == "*":
            blobs = self.client.list_blobs(prefix=id, versions=True)
            blobs_to_delete = []
            for blob in blobs:
                if blob.name == id:
                    blobs_to_delete.append(blob)
            if len(blobs_to_delete) > 1:
                with self.storage_client.batch():
                    for blob in blobs_to_delete:
                        if blob.name == id:
                            blob.delete()
        return None

    def query(
        self,
        query_args: QueryArgs,
        nargs: Any,
    ) -> Any:
        blobs: list = []
        prefixes: list = []
        args: dict = {}
        args["bucket_or_name"] = self.bucket
        if query_args.prefix is not None:
            args["prefix"] = query_args.prefix
        if query_args.delimiter is not None:
            args["delimiter"] = query_args.delimiter
            args["include_folders_as_prefixes"] = True
        if query_args.start_after is not None:
            args["start_offset"] = f"{query_args.start_after}~"
        if query_args.end_before is not None:
            args["end_offset"] = query_args.end_before
        if query_args.continuation is not None:
            args["page_token"] = query_args.continuation
        if query_args.paging:
            if (
                query_args.limit is not None
                and query_args.page_size is not None
            ):
                if query_args.page_size < query_args.limit:
                    args["max_results"] = query_args.page_size
                else:
                    args["max_results"] = query_args.limit
            elif query_args.limit is not None:
                args["max_results"] = query_args.limit
            elif query_args.page_size is not None:
                args["max_results"] = query_args.page_size
        else:
            if (
                query_args.limit is not None
                and query_args.page_size is not None
            ):
                args["max_results"] = query_args.limit
                args["page_size"] = query_args.page_size
            elif query_args.limit is not None:
                args["max_results"] = query_args.limit
            elif query_args.page_size is not None:
                args["page_size"] = query_args.page_size

        response = NCall(self.storage_client.list_blobs, args, nargs).invoke()
        for blob in response:
            blobs.append(blob)

        for prefix in response.prefixes:
            prefixes.append(prefix)

        prefixes = sorted(prefixes)
        continuation = None
        if query_args.page_size:
            continuation = response.next_page_token

        return dict(blobs=blobs, prefixes=prefixes, continuation=continuation)

    def batch_delete(self, objects: list[dict], nargs: Any) -> Any:
        with self.storage_client.batch():
            for object in objects:
                blob = self.client.blob(object["id"])
                if object["version"] is not None:
                    blob.generation = int(object["version"])
                blob.delete()


class ResultConverter:
    processor: ItemProcessor
    bucket: str

    def __init__(self, processor: ItemProcessor, bucket: str):
        self.processor = processor
        self.bucket = bucket

    def convert_storage_class(self, storage_class: str | None) -> str:
        map = {
            "STANDARD": ObjectStoreClass.HOT,
            "NEARLINE": ObjectStoreClass.COOL,
            "COLDLINE": ObjectStoreClass.COLD,
            "ARCHIVE": ObjectStoreClass.ARCHIVE,
        }
        if storage_class is None:
            return ObjectStoreClass.HOT.value
        if storage_class in map:
            return map[storage_class].value
        return storage_class

    def convert_properties(self, blob: Blob) -> ObjectProperties:
        properties = ObjectProperties()
        properties.cache_control = blob.cache_control
        properties.content_disposition = blob.content_disposition
        properties.content_encoding = blob.content_encoding
        properties.content_language = blob.content_language
        properties.content_length = blob.size
        properties.content_md5 = blob.md5_hash
        properties.content_type = blob.content_type
        properties.crc32c = blob.crc32c
        properties.last_modified = (
            blob.updated.timestamp() if blob.updated is not None else None
        )
        properties.etag = blob.etag
        properties.storage_class = self.convert_storage_class(
            blob.storage_class
        )
        return properties

    def convert_put_result(
        self,
        id: str,
        blob: Blob,
        value: bytes | None,
        returning: str | None,
    ) -> ObjectItem:
        return_value = None
        if returning == "new":
            return_value = value
        return ObjectItem(
            key=ObjectKey(id=id, version=str(blob.generation)),
            value=return_value,
            metadata=blob.metadata,
            properties=self.convert_properties(blob),
            url=blob.public_url,
        )

    def convert_get_result(
        self,
        id: str,
        nresult: Any,
    ) -> ObjectItem:
        value, blob = nresult
        return ObjectItem(
            key=ObjectKey(id=id, version=str(blob.generation)),
            value=value,
            metadata=blob.metadata,
            properties=self.convert_properties(blob),
            url=blob.public_url,
        )

    def convert_get_properties_result(self, id: str, blob: Blob) -> ObjectItem:
        key = ObjectKey(id=id, version=str(blob.generation))
        properties = self.convert_properties(blob)
        return ObjectItem(
            key=key,
            metadata=blob.metadata,
            properties=properties,
            url=blob.public_url,
        )

    def convert_get_versions_result(self, id: str, nresult: Any) -> ObjectItem:
        key = ObjectKey(id=id)
        versions: list = []
        url = None
        for blob in nresult:
            if blob.name != id:
                continue
            properties = self.convert_properties(blob)
            versions.append(
                ObjectVersion(
                    version=str(blob.generation),
                    properties=properties,
                    metadata=blob.metadata,
                )
            )
            url = blob.public_url
        if len(versions) == 0:
            raise NotFoundError
        return ObjectItem(key=key, versions=versions, url=url)

    def convert_update_result(self, id: str, blob: Blob) -> ObjectItem:
        return ObjectItem(
            key=ObjectKey(id=id, version=str(blob.generation)),
            metadata=blob.metadata,
            properties=self.convert_properties(blob),
            url=blob.public_url,
        )

    def convert_copy_result(self, id: str, blob: Blob) -> ObjectItem:
        return ObjectItem(
            key=ObjectKey(id=id, version=str(blob.generation)),
            metadata=blob.metadata,
            properties=self.convert_properties(blob),
            url=blob.public_url,
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
        blobs = nresult["blobs"]
        prefixes = nresult["prefixes"]
        continuation = nresult["continuation"]
        items: list = []
        for blob in blobs:
            items.append(
                ObjectItem(
                    key=ObjectKey(id=blob.name),
                    properties=self.convert_properties(blob),
                    metadata=blob.metadata,
                    url=blob.public_url,
                )
            )
        return ObjectList(
            items=items, continuation=continuation, prefixes=prefixes
        )

    def convert_count_result(
        self,
        nresult: Any,
    ) -> int:
        blobs = nresult["blobs"]
        prefixes = nresult["prefixes"]
        return len(blobs) + len(prefixes)


class OperationConverter:
    storage_client: Any
    client: Any
    processor: ItemProcessor
    bucket: str

    def __init__(
        self,
        storage_client: Any,
        client: Any,
        processor: ItemProcessor,
        bucket: str,
    ):
        self.storage_client = storage_client
        self.client = client
        self.processor = processor
        self.bucket = bucket

    @staticmethod
    def decode_etag(etag):
        def decode_metageneration(data):
            if len(data) == 1:
                return data[0]
            result = 0
            for i, byte in enumerate(data[:-1]):
                result += byte * (128 ** (len(data) - i - 1))
            result += data[-1] - 128
            return result

        def decode_generation(data):
            shift = 0
            result = 0

            for i, byte in enumerate(data):
                result |= (byte & 0x7F) << shift
                shift += 7
                if not (byte & 0x80):
                    return result, i + 1
            raise ValueError("Invalid etag encoding")

        try:
            decoded_bytes = base64.b64decode(etag)
            position = 1

            generation, bytes_used = decode_generation(
                decoded_bytes[position:]
            )
            position += bytes_used + 1

            metageneration = decode_metageneration(
                decoded_bytes[position:][::-1]
            )

            return generation, metageneration
        except Exception:
            raise BadRequestError("ETag format error")

    def convert_storage_class(self, storage_class: str | None) -> str:
        map = {
            ObjectStoreClass.HOT: "STANDARD",
            ObjectStoreClass.COOL: "NEARLINE",
            ObjectStoreClass.COLD: "COLDLINE",
            ObjectStoreClass.ARCHIVE: "ARCHIVE",
        }
        if storage_class is None:
            return map[ObjectStoreClass.HOT]
        if ObjectStoreClass(storage_class) in map:
            return map[ObjectStoreClass(storage_class)]
        return storage_class

    def convert_properties_metadata(
        self, blob: Blob, properties: dict | None, metadata: dict | None
    ) -> Blob:
        if properties is None:
            props = ObjectProperties()
        else:
            props = ObjectProperties(**properties)
        if props.cache_control is not None:
            blob.cache_control = props.cache_control
        if props.content_disposition is not None:
            blob.content_disposition = props.content_disposition
        if props.content_encoding is not None:
            blob.content_encoding = props.content_encoding
        if props.content_language is not None:
            blob.content_language = props.content_language
        if props.content_md5 is not None:
            blob.md5_hash = props.content_md5
        if props.content_type is not None:
            blob.content_type = props.content_type
        if props.crc32c is not None:
            blob.crc32c = props.crc32c
        if props.storage_class is not None:
            blob.storage_class = self.convert_storage_class(
                props.storage_class
            )
        if metadata is not None:
            blob.metadata = metadata
        return blob

    def convert_match_condition(
        self, match_condition: MatchCondition, get: bool = False
    ) -> dict:
        args: dict = {}
        if match_condition.exists is False:
            args["if_generation_match"] = 0
        if match_condition.exists is True:
            args["if_generation_not_match"] = 0
        if match_condition.if_match is not None:
            if get:
                args["if_etag_match"] = match_condition.if_match
            else:
                generation, metageneration = OperationConverter.decode_etag(
                    match_condition.if_match
                )
                args["if_generation_match"] = generation
                args["if_metageneration_match"] = metageneration
        if match_condition.if_none_match is not None:
            if get:
                args["if_etag_not_match"] = match_condition.if_none_match
            else:
                generation, metageneration = OperationConverter.decode_etag(
                    match_condition.if_none_match
                )
                args["if_generation_not_match"] = generation
                args["if_metageneration_not_match"] = metageneration
        return args

    def convert_copy_match_condition(
        self, match_condition: MatchCondition
    ) -> dict:
        args: dict = {}
        if match_condition.if_match is not None:
            generation, metageneration = OperationConverter.decode_etag(
                match_condition.if_match
            )
            args["if_generation_match"] = generation
            args["if_metageneration_match"] = metageneration
        if match_condition.if_none_match is not None:
            generation, metageneration = OperationConverter.decode_etag(
                match_condition.if_match
            )
            args["if_generation_not_match"] = generation
            args["if_metageneration_not_match"] = metageneration
        return args

    def convert_put(
        self,
        id: str,
        value: bytes | None,
        file: str | None,
        stream: IO | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        returning: bool | None,
        config: ObjectTransferConfig | None,
    ) -> tuple[
        dict,
        str,
        Blob,
    ]:
        blob = self.client.blob(blob_name=id)
        blob = self.convert_properties_metadata(
            blob=blob, properties=properties, metadata=metadata
        )
        if config is not None and config.multipart:
            func = "helper_put"
            args: dict = {
                "blob": blob,
                "value": value,
                "file": file,
                "stream": stream,
                "match_condition": match_condition,
                "returning": returning,
                "config": config,
            }
        else:
            args = {}
            if value is not None:
                func = "upload_from_file"
                args["file_obj"] = BytesIO(value)
            elif stream is not None:
                func = "upload_from_file"
                args["file_obj"] = stream
            elif file is not None:
                func = "upload_from_filename"
                args["filename"] = file
            args = args | self.convert_match_condition(match_condition)
        return args, func, blob

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
        blob: Blob,
    ) -> tuple[dict, str]:
        if config is not None and config.multipart:
            func = "helper_get"
            args: dict = {
                "blob": blob,
                "file": file,
                "stream": stream,
                "match_condition": match_condition,
                "config": config,
            }
        else:
            args = {}
            if start is not None:
                args["start"] = start
            if end is not None:
                args["end"] = end
            args = args | self.convert_match_condition(match_condition, True)
            if file is not None:
                args["filename"] = file
                func = "download_to_filename"
            elif stream is not None:
                args["file_obj"] = stream
                func = "download_to_file"
            else:
                func = "download_as_bytes"
            args = args | self.convert_match_condition(match_condition, True)
        return args, func

    def convert_get_properties(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
    ) -> dict:
        args: dict = {}
        args["blob_name"] = id
        if version is not None:
            args["generation"] = int(version)
        args = args | self.convert_match_condition(match_condition, True)
        return args

    def convert_get_versions(self, id: str) -> dict:
        return {"prefix": id, "versions": True}

    def convert_update(
        self,
        id: str,
        version: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
    ) -> tuple[dict, Blob, bool, bool]:
        update_storage_class = False
        update_others = False
        blob = self.client.blob(blob_name=id)
        if version is not None:
            blob.generation = int(version)
        blob = self.convert_properties_metadata(
            blob=blob, properties=properties, metadata=metadata
        )
        if blob.storage_class is not None:
            update_storage_class = True
            if (
                properties is not None and len(properties) > 1
            ) or metadata is not None:
                # updating more than storage class
                update_others = True
        else:
            update_others = True
        args = self.convert_match_condition(match_condition)
        return args, blob, update_storage_class, update_others

    def convert_delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
    ) -> tuple[dict, str]:
        if version == "*":
            args: dict = {
                "id": id,
                "version": version,
                "match_condition": match_condition,
            }
            return args, "helper_delete"
        args = {"blob_name": id}
        if version is not None:
            args["generation"] = int(version)
        else:
            args["generation"] = 0
        args = args | self.convert_match_condition(match_condition)
        return args, "delete_blob"

    def convert_copy(
        self,
        id: str,
        source_id: str,
        source_version: str | None,
        source_collection: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
    ) -> tuple[dict, Bucket]:
        args: dict = {}
        source_bucket = self.storage_client.bucket(
            bucket_name=(
                source_collection
                if source_collection is not None
                else self.bucket
            )
        )
        blob = source_bucket.blob(blob_name=source_id)
        blob = self.convert_properties_metadata(blob, properties, metadata)
        args["blob"] = blob
        args["destination_bucket"] = self.client
        args["new_name"] = id
        if source_version is not None:
            args["source_generation"] = int(source_version)
        args = args | self.convert_copy_match_condition(match_condition)
        return args, source_bucket

    def convert_generate(
        self,
        id: str,
        version: str | None,
        method: str | None,
        expiry: int | None,
    ) -> tuple[dict, Blob]:
        args: dict = {}
        blob = self.client.blob(blob_name=id)
        if version is not None:
            args["generation"] = int(version)
        args["version"] = "v4"
        args["method"] = method if method is not None else "GET"
        args["expiration"] = timedelta(
            seconds=expiry if expiry is not None else 3600
        )
        return args, blob

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
        objects = []
        for op_parser in op_parsers:
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            objects.append({"id": id, "version": version})
        return {"objects": objects}


class GoogleCloudStorageCollection:
    client: Any
    helper: Any
    op_converter: OperationConverter
    result_converter: ResultConverter
    processor: ItemProcessor

    def __init__(
        self,
        storage_client: Any,
        client: Any,
        bucket: str,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.op_converter = OperationConverter(
            storage_client, self.client, self.processor, bucket
        )
        self.helper = ClientHelper(
            storage_client, client, bucket, self.op_converter
        )
        self.result_converter = ResultConverter(self.processor, bucket)
