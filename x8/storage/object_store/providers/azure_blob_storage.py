"""
Object Store on Azure Blob Storage.
"""

from __future__ import annotations

__all__ = ["AzureBlobStorage"]

import binascii
from datetime import datetime, timedelta, timezone
from typing import IO, Any

from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.storage.blob import (
    BlobSasPermissions,
    ContentSettings,
    StandardBlobTier,
    generate_blob_sas,
)
from x8._common.azure_provider import AzureProvider
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


class AzureBlobStorage(AzureProvider, StoreProvider):
    container: str | None
    connection_string: str | None
    account_url: str | None
    account_key: str | None
    sas_token: str | None
    nparams: dict[str, Any]

    _credential: Any
    _acredential: Any
    _service_client: Any
    _aservice_client: Any

    _collection_cache: dict[str, AzureBlobStorageCollection]
    _acollection_cache: dict[str, AzureBlobStorageCollection]

    def __init__(
        self,
        container: str | None = None,
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
        **kwargs,
    ):
        """Initialize.

        Args:
            container:
                Azure blob storage container
                mapped to object store collection.
            connection_string:
                Azure blob storage connection string.
            account_url:
                Azure blob storage account url.
            account_key:
                Azure blob storage account key.
            sas_token:
                SAS token.
            credential_type:
                Azure credential type.
                Credential types are default, client_secret, certificate,
                azure_cli, shared_token_cache, managed_identity.
            tenant_id:
                Azure tenant id for client_secret credential type.
            client_id:
                Azure client id for client_secret credential type.
            client_secret:
                Azure client secret for client_secret credential type.
            certificate_path:
                Certificate path for certificate credential type.
            nparams:
                Native parameters to Azure blob storage client.
        """
        self.container = container
        self.connection_string = connection_string
        self.account_url = account_url
        self.account_key = account_key
        self.sas_token = sas_token
        self.nparams = nparams

        self._service_client = None
        self._aservice_client = None
        self._collection_cache = dict()
        self._acollection_cache = dict()
        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._service_client is not None:
            return

        from azure.storage.blob import BlobServiceClient

        (
            self._credential,
            self._service_client,
        ) = self._init_credential_client(
            BlobServiceClient,
            self._get_credential,
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aservice_client is not None:
            return

        from azure.storage.blob.aio import BlobServiceClient

        (
            self._acredential,
            self._aservice_client,
        ) = self._init_credential_client(
            BlobServiceClient,
            self._aget_credential,
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

    def _get_container_name(
        self, op_parser: StoreOperationParser
    ) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        container = (
            collection_name if collection_name is not None else self.container
        )
        return container

    def _get_collection(
        self, op_parser: StoreOperationParser
    ) -> AzureBlobStorageCollection | None:
        if op_parser.is_resource_op():
            return None
        container_name = self._get_container_name(op_parser)
        if container_name is None:
            raise BadRequestError("Collection name must be specified")
        if container_name in self._collection_cache:
            return self._collection_cache[container_name]

        from azure.storage.blob import BlobClient

        client = self._service_client.get_container_client(container_name)
        col = AzureBlobStorageCollection(
            self._service_client,
            client,
            ClientHelper,
            container_name,
            BlobClient,
        )
        self._collection_cache[container_name] = col
        return col

    async def _aget_collection(
        self, op_parser: StoreOperationParser
    ) -> AzureBlobStorageCollection | None:
        if op_parser.is_resource_op():
            return None
        container_name = self._get_container_name(op_parser)
        if container_name is None:
            raise BadRequestError("Collection name must be specified")
        if container_name in self._acollection_cache:
            return self._acollection_cache[container_name]

        from azure.storage.blob.aio import BlobClient

        client = self._aservice_client.get_container_client(container_name)
        col = AzureBlobStorageCollection(
            self._aservice_client,
            client,
            AsyncClientHelper,
            container_name,
            BlobClient,
        )
        self._acollection_cache[container_name] = col
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
            ResourceHelper(self._service_client, self._credential),
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

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        collection = await self._aget_collection(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collection,
            AsyncResourceHelper(self._aservice_client, self._acredential),
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(nresult, state, op_parser, collection)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: AzureBlobStorageCollection | None,
        resource_helper: Any,
    ) -> tuple[NCall | None, dict]:
        if collection is not None:
            op_converter = collection.op_converter
            client = collection.client
            helper = collection.helper
        call = None
        state: dict = {}
        nargs = op_parser.get_nargs()

        def get_error_map(match_condition: MatchCondition):
            return {
                ResourceModifiedError: (
                    PreconditionFailedError
                    if match_condition.exists is True
                    or match_condition.exists is False
                    or match_condition.if_match
                    or match_condition.if_unmodified_since
                    else NotModified
                ),
                ResourceNotFoundError: NotFoundError,
                ResourceExistsError: PreconditionFailedError,
            }

        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            args = {
                "container": self._get_container_name(op_parser),
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
                "container": self._get_container_name(op_parser),
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
                "container": self._get_container_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.has_collection,
                args,
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            match_condition = op_parser.get_match_condition()
            args, blob_client, state = op_converter.convert_put(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value_as_bytes(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=match_condition,
                returning=op_parser.get_returning_as_bool(),
                config=get_transfer_config(op_parser),
            )
            call = NCall(
                blob_client.upload_blob,
                args,
                nargs,
                get_error_map(match_condition),
            )
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            match_condition = op_parser.get_match_condition()
            args, blob_client = op_converter.convert_get(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                match_condition=match_condition,
                start=op_parser.get_start(),
                end=op_parser.get_end(),
                config=get_transfer_config(op_parser),
            )
            call = NCall(
                helper.get,
                {
                    "blob_client": blob_client,
                    "args": args,
                    "file": op_parser.get_file(),
                    "stream": op_parser.get_stream(),
                    "nargs": nargs,
                },
                None,
                get_error_map(match_condition),
            )
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            match_condition = op_parser.get_match_condition()
            args, blob_client = op_converter.convert_get_properties(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=match_condition,
            )
            call = NCall(
                blob_client.get_blob_properties,
                args,
                nargs,
                get_error_map(match_condition),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            args = op_converter.convert_get_versions(
                id=op_parser.get_id_as_str()
            )
            call = NCall(
                helper.get_versions, {"args": args, "nargs": nargs}, None
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            match_condition = op_parser.get_match_condition()
            args = {
                "id": op_parser.get_id_as_str(),
                "version": op_parser.get_version(),
                "metadata": op_parser.get_metadata(),
                "properties": op_parser.get_properties(),
                "match_condition": match_condition,
                "nargs": nargs,
            }
            call = NCall(
                helper.update,
                args,
                nargs,
                get_error_map(match_condition),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            match_condition = op_parser.get_match_condition()
            args, func = op_converter.convert_delete(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=match_condition,
            )
            if func == "helper_delete":
                args["nargs"] = nargs
                call = NCall(
                    helper.delete,
                    args,
                    None,
                    get_error_map(match_condition),
                )
            elif func == "delete_blob":
                call = NCall(
                    client.delete_blob,
                    args,
                    nargs,
                    get_error_map(match_condition),
                )
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            match_condition = op_parser.get_match_condition()
            args = {
                "id": op_parser.get_id_as_str(),
                "source_id": op_parser.get_source_id_as_str(),
                "source_version": op_parser.get_source_version(),
                "source_collection": op_parser.get_source_collection(),
                "metadata": op_parser.get_metadata(),
                "properties": op_parser.get_properties(),
                "match_condition": match_condition,
                "nargs": nargs,
            }
            call = NCall(
                helper.copy,
                args,
                nargs,
                get_error_map(match_condition),
            )
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            args = op_converter.convert_generate(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                method=op_parser.get_method(),
                expiry=op_parser.get_expiry_in_seconds(),
            )
            call = NCall(helper.generate, {"args": args, "nargs": nargs}, None)
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
                call = NCall(helper.batch_delete, args, nargs)
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
        collection: AzureBlobStorageCollection | None,
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
            result = [c.name for c in nresult]
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
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
            result = result_converter.convert_get_result(
                op_parser.get_id_as_str(),
                op_parser.get_file(),
                op_parser.get_stream(),
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
            result = result_converter.convert_update_result(
                op_parser.get_id_as_str(), nresult
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
    service_client: Any
    credential: Any

    def __init__(self, service_client: Any, credential: Any):
        self.service_client = service_client
        self.credential = credential

    def create_collection(
        self,
        container: str,
        config: ObjectCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {
            "name": container,
        }
        if config is not None and config.acl is not None:
            if config.acl == "public-read":
                args["public_access"] = "blob"
        if config is not None and config.nconfig is not None:
            args = args | config.nconfig
        try:
            NCall(
                self.service_client.create_container,
                args,
                nargs,
            ).invoke()
        except ResourceExistsError:
            if exists is False:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        container: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {
            "container": container,
        }
        try:
            NCall(
                self.service_client.delete_container,
                args,
                nargs,
            ).invoke()
        except ResourceNotFoundError:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        response = NCall(
            self.service_client.list_containers, None, nargs
        ).invoke()
        return response

    def has_collection(
        self,
        container: str,
        nargs: Any,
    ):
        container_client = self.service_client.get_container_client(container)
        return NCall(
            container_client.exists,
            None,
            nargs,
        ).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class AsyncResourceHelper:
    service_client: Any
    credential: Any

    def __init__(self, service_client: Any, credential: Any):
        self.service_client = service_client
        self.credential = credential

    async def create_collection(
        self,
        container: str,
        config: ObjectCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ):
        args = {
            "name": container,
        }
        if config is not None and config.acl is not None:
            if config.acl == "public-read":
                args["public_access"] = "blob"
        if config is not None and config.nconfig is not None:
            args = args | config.nconfig
        try:
            await NCall(
                self.service_client.create_container,
                args,
                nargs,
            ).ainvoke()
        except ResourceExistsError:
            if exists is False:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    async def drop_collection(
        self,
        container: str,
        exists: bool | None,
        nargs: Any,
    ):
        args = {
            "container": container,
        }
        try:
            await NCall(
                self.service_client.delete_container,
                args,
                nargs,
            ).ainvoke()
        except ResourceNotFoundError:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    async def list_collections(self, nargs) -> Any:
        nresult = []
        response = NCall(
            self.service_client.list_containers, None, nargs
        ).invoke()
        async for item in response:
            nresult.append(item)
        return nresult

    async def has_collection(
        self,
        container: str,
        nargs: Any,
    ):
        container_client = self.service_client.get_container_client(container)
        return await NCall(
            container_client.exists,
            None,
            nargs,
        ).ainvoke()

    async def close(self, nargs: Any) -> Any:
        await NCall(
            self.service_client.close,
            None,
            nargs,
        ).ainvoke()
        if self.credential is not None:
            await NCall(
                self.credential.close,
                None,
                nargs,
            ).ainvoke()


class ClientHelper:
    service_client: Any
    client: Any
    container: str
    op_converter: OperationConverter

    def __init__(
        self,
        service_client: Any,
        client: Any,
        container: str,
        op_converter: OperationConverter,
    ):
        self.service_client = service_client
        self.client = client
        self.container = container
        self.op_converter = op_converter

    def get(
        self,
        blob_client: Any,
        args: dict,
        file: str | None,
        stream: IO | None,
        nargs: Any,
    ) -> Any:
        nresult = NCall(blob_client.download_blob, args, nargs).invoke()
        value = None
        if file is not None:
            with open(file=file, mode="wb") as f:
                f.write(nresult.readall())
        elif stream is not None:
            nresult.readinto(stream)
        if file is None and stream is None:
            value = nresult.readall()
        return {"blob": nresult, "value": value}

    def get_versions(self, args: dict, nargs: Any):
        nresult = NCall(self.client.list_blobs, args, nargs).invoke()
        return [blob for blob in nresult]

    def generate(self, args: dict, nargs: Any) -> Any:
        return NCall(generate_blob_sas, args, nargs).invoke()

    def delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        nargs: Any,
    ) -> Any:
        args: dict = {"blob": id}
        args = args | self.op_converter.convert_match_condition(
            match_condition
        )
        NCall(self.client.delete_blob, args, nargs).invoke()
        args = {"name_starts_with": id, "include": ["versions"]}
        blobs = NCall(self.client.list_blobs, args, None).invoke()
        blob_properties = []
        for blob in blobs:
            if blob.name == id:
                blob_properties.append(blob)
        NCall(self.client.delete_blobs, blob_properties, nargs).invoke()
        return None

    def copy(
        self,
        id: str,
        source_id: str,
        source_version: str | None,
        source_collection: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        nargs: Any,
    ):
        sas = NCall(
            generate_blob_sas,
            {
                "account_name": self.service_client.account_name,
                "container_name": (
                    source_collection
                    if source_collection is not None
                    else self.container
                ),
                "blob_name": source_id,
                "account_key": self.service_client.credential.account_key,
                "version_id": source_version,
                "permission": BlobSasPermissions(read=True),
                "expiry": datetime.now(timezone.utc) + timedelta(seconds=3600),
            },
            None,
        ).invoke()
        source_blob_client = self.service_client.get_blob_client(
            container=(
                source_collection
                if source_collection is not None
                else self.container
            ),
            blob=source_id,
        )
        source_url = f"{source_blob_client.url}?{sas}"
        args, blob_client, state = self.op_converter.convert_put(
            id,
            value=b"",
            file=None,
            stream=None,
            metadata=metadata,
            properties=properties,
            match_condition=match_condition,
            returning=None,
            config=None,
            copy=True,
        )
        args.pop("data")
        args["source_url"] = source_url
        args["overwrite"] = True
        try:
            return NCall(
                blob_client.upload_blob_from_url, args, nargs
            ).invoke()
        except HttpResponseError as e:
            if e.status_code == 404:
                raise NotFoundError
            if e.status_code == 412:
                raise PreconditionFailedError
            raise

    def update(
        self,
        id: str,
        version: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        nargs: Any,
    ):
        put_args = self.op_converter.convert_properties_metadata(
            properties, metadata
        )
        match_args = self.op_converter.convert_match_condition(match_condition)
        blob_client = self.service_client.get_blob_client(
            container=self.container,
            blob=id,
        )
        response = None
        if "standard_blob_tier" in put_args:
            args = {"standard_blob_tier": put_args["standard_blob_tier"]}
            args = args | match_args
            if version is not None:
                args["version_id"] = version
            response = NCall(
                blob_client.set_standard_blob_tier,
                args,
                nargs,
            ).invoke()
        if "content_settings" in put_args:
            args = {"content_settings": put_args["content_settings"]}
            args = args | match_args
            response = NCall(
                blob_client.set_http_headers,
                args,
                nargs,
            ).invoke()
        if "metadata" in put_args:
            if response is not None and "if_match" in match_args:
                match_args["if_match"] = response["etag"]
            args = {"metadata": put_args["metadata"]}
            args = args | match_args
            response = NCall(
                blob_client.set_blob_metadata, args, nargs
            ).invoke()
        return response

    def query(
        self,
        query_args: QueryArgs,
        nargs: Any,
    ) -> Any:
        from azure.storage.blob import BlobPrefix

        blobs: list = []
        prefixes: list = []
        args: dict = {}
        if query_args.prefix is not None:
            args["name_starts_with"] = query_args.prefix
        if query_args.delimiter is not None:
            args["delimiter"] = query_args.delimiter
        continuation_token = query_args.continuation
        total_key_count = 0
        if query_args.limit is not None and query_args.page_size is not None:
            if query_args.page_size < query_args.limit - total_key_count:
                args["results_per_page"] = query_args.page_size
            else:
                args["results_per_page"] = query_args.limit - total_key_count
        elif query_args.limit is not None:
            args["results_per_page"] = query_args.limit - total_key_count
        elif query_args.page_size is not None:
            args["results_per_page"] = query_args.page_size
        if query_args.delimiter is not None:
            iterator = NCall(self.client.walk_blobs, args, nargs).invoke()
        else:
            iterator = NCall(self.client.list_blobs, args, nargs).invoke()
        iterator_args = {}
        if continuation_token is not None:
            iterator_args["continuation_token"] = continuation_token
        blob_pager = NCall(iterator.by_page, iterator_args, None).invoke()
        for page in blob_pager:
            for b in page:
                if isinstance(b, BlobPrefix):
                    prefixes.append(b.name)
                else:
                    blobs.append(b)
                total_key_count = total_key_count + 1
                if (
                    query_args.limit is not None
                    and total_key_count >= query_args.limit
                ):
                    break
            if (
                query_args.limit is not None
                and total_key_count >= query_args.limit
            ):
                break
            if blob_pager.continuation_token is not None:
                continuation_token = blob_pager.continuation_token
            else:
                continuation_token = None
                break
            if query_args.paging:
                break
        return dict(
            blobs=blobs, prefixes=prefixes, continuation=continuation_token
        )

    def batch_delete(self, blobs: list, nargs: Any) -> None:
        for blob in blobs:
            NCall(
                self.client.delete_blob,
                {"blob": blob["id"], "version_id": blob["version"]},
                nargs,
            ).invoke()


class AsyncClientHelper:
    service_client: Any
    client: Any
    container: str
    op_converter: OperationConverter

    def __init__(
        self,
        service_client: Any,
        client: Any,
        container: str,
        op_converter: OperationConverter,
    ):
        self.service_client = service_client
        self.client = client
        self.container = container
        self.op_converter = op_converter

    async def get(
        self,
        blob_client: Any,
        args: dict,
        file: str | None,
        stream: IO | None,
        nargs: Any,
    ) -> Any:
        nresult = await NCall(blob_client.download_blob, args, nargs).ainvoke()
        value = None
        if file is not None:
            with open(file=file, mode="wb") as f:
                f.write(await nresult.readall())
        elif stream is not None:
            await nresult.readinto(stream)
        if file is None and stream is None:
            value = await nresult.readall()
        return {"blob": nresult, "value": value}

    async def get_versions(self, args: dict, nargs: Any):
        nresult = NCall(self.client.list_blobs, args, nargs).invoke()
        return [blob async for blob in nresult]

    async def generate(self, args: dict, nargs: Any) -> Any:
        return NCall(generate_blob_sas, args, nargs).invoke()

    async def delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        nargs: Any,
    ) -> Any:
        args: dict = {"blob": id}
        args = args | self.op_converter.convert_match_condition(
            match_condition
        )
        await NCall(self.client.delete_blob, args, nargs).ainvoke()
        args = {"name_starts_with": id, "include": ["versions"]}
        blobs = NCall(self.client.list_blobs, args, None).invoke()
        blob_properties = []
        async for blob in blobs:
            if blob.name == id:
                blob_properties.append(blob)
        await NCall(self.client.delete_blobs, blob_properties, nargs).ainvoke()
        return None

    async def copy(
        self,
        id: str,
        source_id: str,
        source_version: str | None,
        source_collection: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        nargs: Any,
    ):
        sas = NCall(
            generate_blob_sas,
            {
                "account_name": self.service_client.account_name,
                "container_name": (
                    source_collection
                    if source_collection is not None
                    else self.container
                ),
                "blob_name": source_id,
                "account_key": self.service_client.credential.account_key,
                "version_id": source_version,
                "permission": BlobSasPermissions(read=True),
                "expiry": datetime.now(timezone.utc) + timedelta(seconds=3600),
            },
            None,
        ).invoke()
        source_blob_client = self.service_client.get_blob_client(
            container=(
                source_collection
                if source_collection is not None
                else self.container
            ),
            blob=source_id,
        )
        source_url = f"{source_blob_client.url}?{sas}"
        args, blob_client, state = self.op_converter.convert_put(
            id,
            value=b"",
            file=None,
            stream=None,
            metadata=metadata,
            properties=properties,
            match_condition=match_condition,
            returning=None,
            config=None,
            copy=True,
        )
        args.pop("data")
        args["source_url"] = source_url
        args["overwrite"] = True
        try:
            return await NCall(
                blob_client.upload_blob_from_url, args, nargs
            ).ainvoke()
        except HttpResponseError as e:
            if e.status_code == 404:
                raise NotFoundError
            if e.status_code == 412:
                raise PreconditionFailedError
            raise

    async def update(
        self,
        id: str,
        version: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        nargs: Any,
    ):
        put_args = self.op_converter.convert_properties_metadata(
            properties, metadata
        )
        match_args = self.op_converter.convert_match_condition(match_condition)
        blob_client = self.service_client.get_blob_client(
            container=self.container,
            blob=id,
        )
        response = None
        if "standard_blob_tier" in put_args:
            args = {"standard_blob_tier": put_args["standard_blob_tier"]}
            args = args | match_args
            if version is not None:
                args["version_id"] = version
            response = await NCall(
                blob_client.set_standard_blob_tier,
                args,
                nargs,
            ).ainvoke()
        if "content_settings" in put_args:
            args = {"content_settings": put_args["content_settings"]}
            args = args | match_args
            response = await NCall(
                blob_client.set_http_headers,
                args,
                nargs,
            ).ainvoke()
        if "metadata" in put_args:
            if response is not None and "if_match" in match_args:
                match_args["if_match"] = response["etag"]
            args = {"metadata": put_args["metadata"]}
            args = args | match_args
            response = await NCall(
                blob_client.set_blob_metadata, args, nargs
            ).ainvoke()
        return response

    async def query(
        self,
        query_args: QueryArgs,
        nargs: Any,
    ) -> Any:
        from azure.storage.blob.aio import BlobPrefix

        blobs: list = []
        prefixes: list = []
        args: dict = {}
        if query_args.prefix is not None:
            args["name_starts_with"] = query_args.prefix
        if query_args.delimiter is not None:
            args["delimiter"] = query_args.delimiter
        continuation_token = query_args.continuation
        total_key_count = 0
        if query_args.limit is not None and query_args.page_size is not None:
            if query_args.page_size < query_args.limit - total_key_count:
                args["results_per_page"] = query_args.page_size
            else:
                args["results_per_page"] = query_args.limit - total_key_count
        elif query_args.limit is not None:
            args["results_per_page"] = query_args.limit - total_key_count
        elif query_args.page_size is not None:
            args["results_per_page"] = query_args.page_size
        if query_args.delimiter is not None:
            iterator = NCall(self.client.walk_blobs, args, nargs).invoke()
        else:
            iterator = NCall(self.client.list_blobs, args, nargs).invoke()
        iterator_args = {}
        if continuation_token is not None:
            iterator_args["continuation_token"] = continuation_token
        blob_pager = NCall(iterator.by_page, iterator_args, None).invoke()

        async for page in blob_pager:
            async for b in page:
                if isinstance(b, BlobPrefix):
                    prefixes.append(b.name)
                else:
                    blobs.append(b)
                total_key_count = total_key_count + 1
                if (
                    query_args.limit is not None
                    and total_key_count >= query_args.limit
                ):
                    break
            if (
                query_args.limit is not None
                and total_key_count >= query_args.limit
            ):
                break
            if blob_pager.continuation_token is not None:
                continuation_token = blob_pager.continuation_token
            else:
                continuation_token = None
                break
            if query_args.paging:
                break
        return dict(
            blobs=blobs, prefixes=prefixes, continuation=continuation_token
        )

    async def batch_delete(self, blobs: list, nargs: Any) -> None:
        import asyncio

        tasks = []
        for blob in blobs:
            tasks.append(
                NCall(
                    self.client.delete_blob,
                    {"blob": blob["id"], "version_id": blob["version"]},
                    nargs,
                ).ainvoke()
            )
        await asyncio.gather(*tasks)


class ResultConverter:
    service_client: Any
    processor: ItemProcessor
    container: str

    def __init__(
        self, service_client: Any, processor: ItemProcessor, container: str
    ):
        self.service_client = service_client
        self.processor = processor
        self.container = container

    def convert_md5(self, md5: bytearray | None) -> str | None:
        if md5 is None:
            return None
        blobmd5 = bytearray(md5)
        hex = binascii.hexlify(blobmd5).decode("utf-8")
        return hex

    def convert_url(self, id: str):
        return f"{self.service_client.url}{self.container}/{id}"

    def convert_storage_class(self, storage_class: str | None) -> str:
        map = {
            "Hot": ObjectStoreClass.HOT,
            "Cool": ObjectStoreClass.COOL,
            "Cold": ObjectStoreClass.COLD,
            "Archive": ObjectStoreClass.ARCHIVE,
        }
        if storage_class is None:
            return ObjectStoreClass.HOT.value
        if storage_class in map:
            return map[storage_class].value
        return storage_class

    def convert_properties(self, nresult: Any) -> ObjectProperties:
        properties = ObjectProperties()
        properties.cache_control = nresult.content_settings.cache_control
        properties.content_disposition = (
            nresult.content_settings.content_disposition
        )
        properties.content_encoding = nresult.content_settings.content_encoding
        properties.content_language = nresult.content_settings.content_language
        properties.content_md5 = self.convert_md5(
            nresult.content_settings.content_md5
        )
        properties.content_type = nresult.content_settings.content_type
        properties.content_length = nresult.size
        properties.last_modified = nresult.last_modified.timestamp()
        properties.etag = nresult.etag
        properties.storage_class = self.convert_storage_class(
            nresult.blob_tier
        )
        return properties

    def convert_put_result(
        self,
        id: str,
        nresult: Any,
        value: bytes | None,
        returning: str | None,
    ) -> ObjectItem:
        version = None
        etag = None
        last_modified = None
        content_md5 = None
        if nresult is not None and isinstance(nresult, dict):
            version = nresult.get("version_id", None)
            etag = nresult.get("etag", None)
            last_modified = nresult.get("last_modified", None)
            if last_modified is not None:
                last_modified = last_modified.timestamp()
            content_md5 = nresult.get("content_md5", None)
        return_value = None
        if returning == "new":
            return_value = value
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            value=return_value,
            properties=ObjectProperties(
                etag=etag,
                last_modified=last_modified,
                content_md5=self.convert_md5(content_md5),
            ),
            url=self.convert_url(id=id),
        )

    def convert_copy_result(self, id: str, nresult: Any) -> ObjectItem:
        version = None
        etag = None
        last_modified = None
        content_md5 = None
        metadata = None
        if nresult is not None and isinstance(nresult, dict):
            version = nresult.get("version_id", None)
            etag = nresult.get("etag", None)
            last_modified = nresult.get("last_modified", None)
            if last_modified is not None:
                last_modified = last_modified.timestamp()
            content_md5 = nresult.get("content_md5", None)
            metadata = nresult.get("metadata", None)
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            metadata=metadata,
            properties=ObjectProperties(
                etag=etag,
                last_modified=last_modified,
                content_md5=self.convert_md5(content_md5),
            ),
            url=self.convert_url(id=id),
        )

    def convert_get_result(
        self,
        id: str,
        file: str | None,
        stream: IO | None,
        nresult: Any,
    ) -> ObjectItem:
        blob = nresult["blob"]
        value = nresult["value"]
        return ObjectItem(
            key=ObjectKey(id=id, version=blob.properties.version_id),
            value=value,
            metadata=blob.properties.metadata,
            properties=self.convert_properties(blob.properties),
            url=self.convert_url(id),
        )

    def convert_get_properties_result(
        self, id: str, nresult: Any
    ) -> ObjectItem:
        key = ObjectKey(id=id, version=nresult.version_id)
        properties = self.convert_properties(nresult)
        return ObjectItem(
            key=key,
            metadata=nresult.metadata,
            properties=properties,
            url=self.convert_url(id),
        )

    def convert_get_versions_result(self, id: str, nresult: Any) -> ObjectItem:
        key = ObjectKey(id=id)
        versions: list = []
        url = None
        for blob in nresult:
            if blob.name != id or blob.deleted:
                continue
            properties = self.convert_properties(blob)
            versions.append(
                ObjectVersion(
                    version=blob.version_id,
                    properties=properties,
                    metadata=blob.metadata,
                    latest=blob.is_current_version,
                )
            )
            url = self.convert_url(id)
        if len(versions) == 0:
            raise NotFoundError
        return ObjectItem(key=key, versions=versions, url=url)

    def convert_update_result(self, id: str, nresult: Any) -> ObjectItem:
        version = None
        etag = None
        last_modified = None
        if "etag" in nresult:
            etag = nresult["etag"]
        if "last_modified" in nresult:
            last_modified = nresult["last_modified"].timestamp()
        if "version_id" in nresult:
            version = nresult["version_id"]
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            properties=ObjectProperties(
                etag=etag, last_modified=last_modified
            ),
            url=self.convert_url(id),
        )

    def convert_generate_result(
        self, id: str, version: str | None, nresult: Any
    ) -> ObjectItem:
        blob_client = self.service_client.get_blob_client(
            container=self.container, blob=id
        )
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            url=f"{blob_client.url}?{nresult}",
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
                    url=self.convert_url(blob.name),
                )
            )
        return ObjectList(
            items=items,
            continuation=continuation,
            prefixes=prefixes,
        )

    def convert_count_result(
        self,
        nresult: Any,
    ) -> int:
        blobs = nresult["blobs"]
        prefixes = nresult["prefixes"]
        return len(blobs) + len(prefixes)


class OperationConverter:
    service_client: Any
    processor: ItemProcessor
    container: str
    blob_client_type: Any

    def __init__(
        self,
        service_client: Any,
        processor: ItemProcessor,
        container: str,
        blob_client_type: Any,
    ):
        self.service_client = service_client
        self.processor = processor
        self.container = container
        self.blob_client_type = blob_client_type

    def convert_md5(self, md5_str: str | None) -> bytearray | None:
        if md5_str is None:
            return None
        blobmd5 = binascii.unhexlify(md5_str)
        return bytearray(blobmd5)

    def convert_transfer_config_upload_for_client(
        self, config: ObjectTransferConfig | None
    ) -> dict:
        args: dict = {}
        if config is not None:
            if config.multipart is not None:
                args["max_single_put_size"] = 0
            if config.chunksize is not None:
                args["max_block_size"] = config.chunksize
            if config.nconfig is not None:
                args = args | config.nconfig
        return args

    def convert_transfer_config_download_for_client(
        self, config: ObjectTransferConfig | None
    ) -> dict:
        args: dict = {}
        if config is not None:
            if config.multipart is not None:
                args["max_single_get_size"] = 1
            if config.chunksize is not None:
                args["max_chunk_get_size"] = config.chunksize
            if config.nconfig is not None:
                args = args | config.nconfig
        return args

    def convert_transfer_config(
        self, config: ObjectTransferConfig | None
    ) -> dict:
        args: dict = {}
        if config is not None:
            if config.concurrency is not None:
                args["max_concurrency"] = config.concurrency
            if config.nconfig is not None:
                args = args | config.nconfig
        return args

    def convert_storage_class(self, storage_class: str | None) -> str:
        map = {
            ObjectStoreClass.HOT: StandardBlobTier.HOT,
            ObjectStoreClass.COOL: StandardBlobTier.COOL,
            ObjectStoreClass.COLD: StandardBlobTier.COLD,
            ObjectStoreClass.ARCHIVE: StandardBlobTier.ARCHIVE,
        }
        if storage_class is None:
            return map[ObjectStoreClass.HOT]
        if ObjectStoreClass(storage_class) in map:
            return map[ObjectStoreClass(storage_class)]
        return storage_class

    def convert_properties_metadata(
        self, properties: dict | None, metadata: dict | None
    ) -> dict:
        args: dict = {}
        if properties is None:
            props = ObjectProperties()
        else:
            props = ObjectProperties(**properties)
        if (
            props.cache_control is not None
            or props.content_disposition is not None
            or props.content_encoding is not None
            or props.content_language is not None
            or props.content_md5 is not None
            or props.content_type is not None
        ):
            content_settings = ContentSettings()
            if props.cache_control is not None:
                content_settings.cache_control = props.cache_control
            if props.content_disposition is not None:
                content_settings.content_disposition = (
                    props.content_disposition
                )
            if props.content_encoding is not None:
                content_settings.content_encoding = props.content_encoding
            if props.content_language is not None:
                content_settings.content_language = props.content_language
            if props.content_md5 is not None:
                content_settings.content_md5 = self.convert_md5(
                    props.content_md5
                )
            if props.content_type is not None:
                content_settings.content_type = props.content_type
            args["content_settings"] = content_settings
        if props.storage_class is not None:
            args["standard_blob_tier"] = self.convert_storage_class(
                props.storage_class
            )
        if metadata is not None:
            args["metadata"] = metadata
        return args

    def convert_match_condition(self, match_condition: MatchCondition) -> dict:
        args: dict = {}
        if match_condition.exists is True:
            args["if_match"] = "*"
        if match_condition.exists is False:
            args["overwrite"] = False
        if match_condition.if_match is not None:
            args["if_match"] = match_condition.if_match
        if match_condition.if_none_match is not None:
            args["if_none_match"] = match_condition.if_none_match
        if match_condition.if_modified_since is not None:
            args["if_modified_since"] = datetime.fromtimestamp(
                match_condition.if_modified_since, tz=timezone.utc
            )
        if match_condition.if_unmodified_since is not None:
            args["if_unmodified_since"] = datetime.fromtimestamp(
                match_condition.if_unmodified_since, tz=timezone.utc
            )
        return args

    def convert_copy_match_condition(
        self, match_condition: MatchCondition
    ) -> dict:
        args: dict = {}
        if match_condition.exists is True:
            args["source_if_match"] = "*"
        if match_condition.if_match is not None:
            args["source_if_match"] = match_condition.if_match
        if match_condition.if_none_match is not None:
            args["source_if_none_match"] = match_condition.if_none_match
        if match_condition.if_modified_since is not None:
            args["source_if_modified_since"] = datetime.fromtimestamp(
                match_condition.if_modified_since, tz=timezone.utc
            )
        if match_condition.if_unmodified_since is not None:
            args["source_if_unmodified_since"] = datetime.fromtimestamp(
                match_condition.if_unmodified_since, tz=timezone.utc
            )
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
        copy: bool = False,
    ) -> tuple[dict, Any, dict]:
        if config is not None and config.multipart:
            bc_args = {
                "account_url": self.service_client.url,
                "container_name": self.container,
                "blob_name": id,
                "credential": self.service_client.credential,
            }
            bc_args = bc_args | self.convert_transfer_config_upload_for_client(
                config
            )
            blob_client = self.blob_client_type(**bc_args)
        else:
            blob_client = self.service_client.get_blob_client(
                container=self.container, blob=id
            )

        args: dict = {}
        state: dict = {}
        if value is not None:
            args["data"] = value
        elif stream is not None:
            args["data"] = stream
        elif file is not None:
            args["data"] = open(file, "rb")
            state["open_file"] = args["data"]
        args = args | self.convert_properties_metadata(properties, metadata)
        if not copy:
            match_condition_args = self.convert_match_condition(
                match_condition
            )
            args = args | match_condition_args
            if (
                match_condition.exists is None
                and len(match_condition_args) == 0
            ):
                args["overwrite"] = True
        else:
            args = args | self.convert_copy_match_condition(match_condition)
        args = args | self.convert_transfer_config(config)
        return args, blob_client, state

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
    ) -> tuple[dict, Any]:
        if config is not None and config.multipart:
            bc_args = {
                "account_url": self.service_client.url,
                "container_name": self.container,
                "blob_name": id,
                "credential": self.service_client.credential,
            }
            bc_args = (
                bc_args
                | self.convert_transfer_config_download_for_client(config)
            )
            blob_client = self.blob_client_type(**bc_args)
        else:
            blob_client = self.service_client.get_blob_client(
                container=self.container, blob=id
            )

        args: dict = {}
        if start is not None:
            args["offset"] = start
        if end is not None:
            s = start if start is not None else 0
            args["offset"] = s
            args["length"] = end - s + 1
        if version is not None:
            args["version_id"] = version
        args = args | self.convert_match_condition(match_condition)
        args = args | self.convert_transfer_config(config)
        return args, blob_client

    def convert_get_properties(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
    ) -> tuple[dict, Any]:
        args = {}
        blob_client = self.service_client.get_blob_client(
            container=self.container, blob=id
        )
        if version is not None:
            args["version_id"] = version
        args = args | self.convert_match_condition(match_condition)
        return args, blob_client

    def convert_get_versions(self, id: str) -> dict:
        return {"name_starts_with": id, "include": ["versions"]}

    def convert_delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
    ) -> tuple[dict, str]:
        if version == "*":
            args = {
                "id": id,
                "version": version,
                "match_condition": match_condition,
            }
            return args, "helper_delete"
        args = {"blob": id}
        if version is not None:
            args["version_id"] = version
        args = args | self.convert_match_condition(match_condition)
        return args, "delete_blob"

    def convert_generate(
        self,
        id: str,
        version: str | None,
        method: str | None,
        expiry: int | None,
    ):
        args = {}
        args["account_name"] = self.service_client.account_name
        args["container_name"] = self.container
        args["blob_name"] = id
        args["account_key"] = self.service_client.credential.account_key
        if method is None or method == "GET":
            args["permission"] = BlobSasPermissions(read=True)
        elif method == "PUT":
            args["permission"] = BlobSasPermissions(write=True)
        if expiry is not None:
            args["expiry"] = datetime.now(timezone.utc) + timedelta(
                seconds=expiry
            )
        if version is not None:
            args["version_id"] = version
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
        blobs = []
        for op_parser in op_parsers:
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            blobs.append(dict(id=id, version=version))
        return {"blobs": blobs}


class AzureBlobStorageCollection:
    client: Any
    helper: Any
    op_converter: OperationConverter
    result_converter: ResultConverter
    processor: ItemProcessor

    def __init__(
        self,
        service_client: Any,
        client: Any,
        helper_type: Any,
        container: str,
        blob_client_type: Any,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.op_converter = OperationConverter(
            service_client,
            self.processor,
            container,
            blob_client_type,
        )
        self.result_converter = ResultConverter(
            service_client, self.processor, container
        )
        self.helper = helper_type(
            service_client, client, container, self.op_converter
        )
