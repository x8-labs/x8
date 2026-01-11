"""
Secret Store on Google Secret Manager.
"""

__all__ = ["GoogleSecretManager"]

from typing import Any

from google.api_core.exceptions import AlreadyExists, NotFound

from x8._common.google_provider import GoogleProvider
from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import NotFoundError, PreconditionFailedError
from x8.ql import QueryProcessor
from x8.storage._common import (
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)

from .._models import (
    SecretItem,
    SecretKey,
    SecretList,
    SecretProperties,
    SecretVersion,
)


class GoogleSecretManager(GoogleProvider, StoreProvider):
    project: str | None
    nparams: dict[str, Any]

    _credentials: Any
    _client: Any
    _aclient: Any
    _processor: ItemProcessor

    def __init__(
        self,
        project: str | None = None,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            project:
                Google project id.
            service_account_info:
                Google service account info with serialized credentials.
            service_account_file:
                Google service account file with credentials.
            access_token:
                Google access token.
            nparams:
                Native params to Google Secret Manager client.
        """
        self.project = project
        self.nparams = nparams

        self._credentials = None
        self._client = None
        self._aclient = None
        self._processor = ItemProcessor()
        GoogleProvider.__init__(
            self,
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        if self._credentials is None:
            self._credentials = self._get_credentials()

        from google.cloud.secretmanager import SecretManagerServiceClient

        self._client = SecretManagerServiceClient(
            credentials=self._credentials
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        if self._credentials is None:
            self._credentials = self._get_credentials()

        from google.cloud.secretmanager import SecretManagerServiceAsyncClient

        self._aclient = SecretManagerServiceAsyncClient(
            credentials=self._credentials
        )

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall = self._get_ncall(
            op_parser,
            self._client,
            ClientHelper(
                self._client, self.project or self._get_default_project()
            ),
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(nresult, op_parser, self._processor)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall = self._get_ncall(
            op_parser,
            self._aclient,
            AsyncClientHelper(
                self._aclient, self.project or self._get_default_project()
            ),
        )
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(nresult, op_parser, self._processor)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        client: Any,
        helper: Any,
    ) -> NCall | None:
        call = None
        nargs = op_parser.get_nargs()
        project = self.project or self._get_default_project()
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            ver = "latest" if version is None else version
            path = f"projects/{project}/secrets/{id}/versions/{ver}"
            args: dict[str, Any] = {"name": path}
            call = NCall(
                client.access_secret_version,
                args,
                nargs,
                {NotFound: NotFoundError},
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            path = f"projects/{project}/secrets/{id}"
            args = {"name": path}
            call = NCall(
                client.get_secret,
                args,
                nargs,
                {NotFound: NotFoundError},
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            args = {"id": id, "nargs": nargs}
            call = NCall(helper.get_versions, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            metadata = op_parser.get_metadata()
            exists = op_parser.get_where_exists()
            args = {
                "id": id,
                "value": value,
                "metadata": metadata if metadata is not None else {},
                "where_exists": exists,
                "nargs": nargs,
            }
            call = NCall(helper.put, args)
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            path = f"projects/{project}/secrets/{id}"
            args = {
                "request": {
                    "parent": path,
                    "payload": {"data": str(value).encode("utf_8")},
                }
            }
            call = NCall(
                client.add_secret_version,
                args,
                nargs,
                {NotFound: NotFoundError},
            )
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            id = op_parser.get_id_as_str()
            metadata = op_parser.get_metadata()
            path = f"projects/{project}/secrets/{id}"
            args = {
                "request": {
                    "secret": {"name": path, "labels": metadata},
                    "update_mask": {"paths": ["labels"]},
                }
            }
            call = NCall(
                client.update_secret,
                args,
                nargs,
                {NotFound: NotFoundError},
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            path = f"projects/{project}/secrets/{id}"
            args = {"name": path}
            call = NCall(
                client.delete_secret, args, nargs, {NotFound: NotFoundError}
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            path = f"projects/{project}"
            args = {"nargs": nargs}
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            path = f"projects/{project}"
            args = {"nargs": nargs}
            call = NCall(helper.query, args)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(helper.close, args)
        else:
            return None
        return call

    def _convert_nresult(
        self,
        nresult: Any,
        op_parser: StoreOperationParser,
        processor: ItemProcessor,
    ) -> Any:
        result: Any = None
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id, version=nresult.name.split("/")[-1]),
                value=nresult.payload.data.decode("utf-8"),
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id),
                metadata=dict(nresult.labels),
                properties=SecretProperties(
                    created_time=nresult.create_time.timestamp()
                ),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            versions: list = []
            for item in nresult:
                versions.append(
                    SecretVersion(
                        version=item.name.split("/")[-1],
                        created_time=item.create_time.timestamp(),
                    )
                )
            return SecretItem(key=SecretKey(id=id), versions=versions)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id, version=nresult.name.split("/")[-1])
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id, version=nresult.name.split("/")[-1])
            )
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            id = op_parser.get_id_as_str()
            metadata = op_parser.get_metadata()
            result = SecretItem(
                key=SecretKey(id=id),
                metadata=metadata if metadata is not None else {},
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult:
                items.append(
                    SecretItem(
                        key=SecretKey(id=item.name.split("/")[-1]),
                        metadata=dict(item.labels),
                        properties=SecretProperties(
                            created_time=item.create_time.timestamp()
                        ),
                    )
                )
            items = QueryProcessor.query_items(
                items=items,
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
                field_resolver=processor.resolve_root_field,
            )
            result = SecretList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            items = []
            for item in nresult:
                items.append(
                    SecretItem(
                        key=SecretKey(id=item.name.split("/")[-1]),
                        metadata=dict(item.labels),
                        properties=SecretProperties(
                            created_time=item.create_time.timestamp()
                        ),
                    )
                )
            result = QueryProcessor.count_items(
                items=items,
                where=op_parser.get_where(),
                field_resolver=processor.resolve_root_field,
            )
        return result


class ClientHelper:
    client: Any
    project: str

    def __init__(self, client: Any, project: str):
        self.client = client
        self.project = project

    def get_versions(self, id: str, nargs: Any):
        path = f"projects/{self.project}/secrets/{id}"
        return NCall(
            self.client.list_secret_versions,
            {"parent": path},
            nargs,
            {NotFound: NotFoundError},
        ).invoke()

    def put(
        self,
        id: str,
        value: str,
        metadata: dict | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> Any:
        error = None
        if where_exists is None or where_exists is False:
            (response, error) = NCall(
                self.client.create_secret,
                {
                    "request": {
                        "parent": f"projects/{self.project}",
                        "secret_id": id,
                        "secret": {
                            "replication": {"automatic": {}},
                            "labels": metadata,
                        },
                    }
                },
                nargs,
                {
                    AlreadyExists: (
                        PreconditionFailedError
                        if where_exists is False
                        else None
                    )
                },
            ).invoke(return_error=True)
        path = f"projects/{self.project}/secrets/{id}"
        response = NCall(
            self.client.add_secret_version,
            {
                "request": {
                    "parent": path,
                    "payload": {"data": value.encode("utf_8")},
                }
            },
            nargs,
            {NotFound: PreconditionFailedError},
        ).invoke()
        if error is not None or where_exists is True:
            NCall(
                self.client.update_secret,
                {
                    "request": {
                        "secret": {"name": path, "labels": metadata},
                        "update_mask": {"paths": ["labels"]},
                    }
                },
                nargs,
            ).invoke()
        return response

    def query(self, nargs: Any):
        path = f"projects/{self.project}"
        response = NCall(
            self.client.list_secrets,
            {"request": {"parent": path}},
            nargs,
        ).invoke()
        return response

    def close(self, nargs: Any) -> Any:
        pass


class AsyncClientHelper:
    client: Any
    project: str

    def __init__(self, client: Any, project: str):
        self.client = client
        self.project = project

    async def get_versions(self, id: str, nargs: Any):
        path = f"projects/{self.project}/secrets/{id}"
        response = await NCall(
            self.client.list_secret_versions,
            {"parent": path},
            nargs,
            {NotFound: NotFoundError},
        ).ainvoke()
        nresult = []
        async for item in response:
            nresult.append(item)
        return nresult

    async def put(
        self,
        id: str,
        value: str,
        metadata: dict | None,
        where_exists: bool,
        nargs: Any,
    ) -> Any:
        error = None
        if where_exists is None or where_exists is False:
            (response, error) = await NCall(
                self.client.create_secret,
                {
                    "request": {
                        "parent": f"projects/{self.project}",
                        "secret_id": id,
                        "secret": {
                            "replication": {"automatic": {}},
                            "labels": metadata,
                        },
                    }
                },
                nargs,
                {
                    AlreadyExists: (
                        PreconditionFailedError
                        if where_exists is False
                        else None
                    )
                },
            ).ainvoke(return_error=True)
        path = f"projects/{self.project}/secrets/{id}"
        response = await NCall(
            self.client.add_secret_version,
            {
                "request": {
                    "parent": path,
                    "payload": {"data": value.encode("utf_8")},
                }
            },
            nargs,
            {NotFound: PreconditionFailedError},
        ).ainvoke()
        if error is not None or where_exists is True:
            await NCall(
                self.client.update_secret,
                {
                    "request": {
                        "secret": {"name": path, "labels": metadata},
                        "update_mask": {"paths": ["labels"]},
                    }
                },
                nargs,
            ).ainvoke()
        return response

    async def query(self, nargs: Any):
        path = f"projects/{self.project}"
        response = await NCall(
            self.client.list_secrets,
            {"request": {"parent": path}},
            nargs,
        ).ainvoke()
        nresult = []
        async for item in response:
            nresult.append(item)
        return nresult

    async def close(self, nargs: Any):
        pass
