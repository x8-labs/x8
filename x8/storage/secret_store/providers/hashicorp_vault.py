"""
Secret Store on HashiCorp Vault.
"""

__all__ = ["HashiCorpVault"]

from datetime import datetime
from typing import Any

import hvac
from hvac.exceptions import InvalidPath, InvalidRequest

from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
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


class HashiCorpVault(StoreProvider):
    url: str
    token: str | None
    role_id: str | None
    secret_id: str | None
    namespace: str | None
    value_field: str
    nparams: dict[str, Any]

    _client: Any
    _processor: ItemProcessor

    def __init__(
        self,
        url: str,
        token: str | None = None,
        role_id: str | None = None,
        secret_id: str | None = None,
        namespace: str | None = "admin",
        value_field: str = "value",
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            url:
                Vault url.
            token:
                Vault token.
            role_id:
                Vault role id.
            secret_id:
                Vault secret id.
            namespace:
                Vault namespace. Defaults to "admin".
            value_field:
                Value field name to store the secret value.
                Defaults to "value".
            nparams:
                Native parameters to vault client.
        """
        self.url = url
        self.token = token
        self.role_id = role_id
        self.secret_id = secret_id
        self.namespace = namespace
        self.value_field = value_field
        self.nparams = nparams

        self._client = None
        self._processor = ItemProcessor()

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        if self.token is not None:
            self._client = hvac.Client(
                url=self.url,
                namespace=self.namespace,
                token=self.token,
                **self.nparams,
            )
        elif self.role_id is not None or self.secret_id is not None:
            self._client = hvac.Client(
                url=self.url,
                namespace=self.namespace,
                **self.nparams,
            )
            auth_response = self._client.auth.approle.login(
                role_id=self.role_id,
                secret_id=self.secret_id,
            )
            token = auth_response["auth"]["client_token"]
            self._client.token = token

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
                self._client,
                self.value_field,
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

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        client: Any,
        helper: Any,
    ) -> NCall | None:
        call = None
        nargs = op_parser.get_nargs()
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            args: dict[str, Any] = {
                "path": id,
                "version": version,
                "raise_on_deleted_version": True,
            }
            call = NCall(
                client.secrets.kv.read_secret_version,
                args,
                nargs,
                {InvalidPath: NotFoundError},
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            args = {
                "path": id,
                "raise_on_deleted_version": True,
            }
            call = NCall(
                client.secrets.kv.read_secret,
                args,
                nargs,
                {InvalidPath: NotFoundError},
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            args = {"path": id}
            call = NCall(
                client.secrets.kv.v2.read_secret_metadata,
                args,
                nargs,
                {InvalidPath: NotFoundError},
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            metadata = op_parser.get_metadata()
            exists = op_parser.get_where_exists()
            args = {
                "path": id,
                "secret": dict({self.value_field: value}),
            }
            if metadata is None and exists is None:
                call = NCall(
                    client.secrets.kv.v2.create_or_update_secret,
                    args,
                    nargs,
                )
            elif metadata is None and exists is False:
                args["cas"] = 0
                call = NCall(
                    client.secrets.kv.v2.create_or_update_secret,
                    args,
                    nargs,
                    {InvalidRequest: PreconditionFailedError},
                )
            else:
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
            args = {"id": id, "value": value, "nargs": nargs}
            call = NCall(
                helper.update,
                args,
            )
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            id = op_parser.get_id_as_str()
            metadata = op_parser.get_metadata()
            args = {
                "id": id,
                "metadata": metadata if metadata is not None else {},
                "nargs": nargs,
            }
            call = NCall(
                helper.update_metadata,
                args,
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            args = {"id": id, "nargs": nargs}
            call = NCall(helper.delete, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = {"path": ""}
            call = NCall(
                client.secrets.kv.v2.list_secrets,
                args,
                nargs,
                {InvalidPath: None},
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = {"path": ""}
            call = NCall(
                client.secrets.kv.v2.list_secrets,
                args,
                nargs,
                {InvalidPath: None},
            )
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
                key=SecretKey(
                    id=id, version=str(nresult["data"]["metadata"]["version"])
                ),
                value=(
                    nresult["data"]["data"][self.value_field]
                    if self.value_field in nresult["data"]["data"]
                    else None
                ),
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            metadata: dict | None = {}
            if nresult["data"]["metadata"]["custom_metadata"] is not None:
                metadata = nresult["data"]["metadata"]["custom_metadata"]
            result = SecretItem(
                key=SecretKey(id=id),
                metadata=metadata if metadata is not None else {},
                properties=SecretProperties(
                    created_time=Helper.convert_to_timestamp(
                        nresult["data"]["metadata"]["created_time"]
                    )
                ),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            versions: list = []
            for key, value in nresult["data"]["versions"].items():
                versions.append(
                    SecretVersion(
                        version=str(key),
                        created_time=Helper.convert_to_timestamp(
                            value["created_time"]
                        ),
                    )
                )
            versions.sort(key=lambda s: int(s.version), reverse=True)
            return SecretItem(key=SecretKey(id=id), versions=versions)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id, version=str(nresult["data"]["version"]))
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            id = op_parser.get_id_as_str()
            result = SecretItem(
                key=SecretKey(id=id, version=str(nresult["data"]["version"]))
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
            if nresult is not None:
                for id in nresult["data"]["keys"]:
                    items.append(SecretItem(key=SecretKey(id=id)))
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
            if nresult is not None:
                for id in nresult["data"]["keys"]:
                    items.append(SecretItem(key=SecretKey(id=id)))
            result = QueryProcessor.count_items(
                items=items,
                where=op_parser.get_where(),
                field_resolver=processor.resolve_root_field,
            )
        return result


class Helper:
    @staticmethod
    def convert_to_timestamp(timestamp: str):
        return (
            datetime.strptime(
                timestamp[:-4],
                "%Y-%m-%dT%H:%M:%S.%f",
            )
            .astimezone()
            .timestamp()
        )


class ClientHelper:
    client: Any
    value_field: str

    def __init__(self, client: Any, value_field: str):
        self.client = client
        self.value_field = value_field

    def put(
        self,
        id: str,
        value: str,
        metadata: dict | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> Any:
        if where_exists is True:
            NCall(
                self.client.secrets.kv.read_secret,
                {"path": id, "raise_on_deleted_version": True},
                nargs,
                {InvalidPath: PreconditionFailedError},
            ).invoke()
        if metadata is not None:
            response = NCall(
                self.client.secrets.kv.v2.create_or_update_secret,
                (
                    {
                        "path": id,
                        "secret": dict({self.value_field: value}),
                        "cas": 0,
                    }
                    if where_exists is False
                    else {
                        "path": id,
                        "secret": dict({self.value_field: value}),
                    }
                ),
                nargs,
                (
                    {InvalidRequest: PreconditionFailedError}
                    if where_exists is False
                    else None
                ),
            ).invoke()
            NCall(
                self.client.secrets.kv.v2.update_metadata,
                {"path": id, "custom_metadata": metadata},
                nargs,
            ).invoke()
            return response
        raise BadRequestError("Operation not supported")

    def update(self, id: str, value: str, nargs: Any) -> Any:
        NCall(
            self.client.secrets.kv.read_secret,
            {"path": id, "raise_on_deleted_version": True},
            nargs,
            {InvalidPath: NotFoundError},
        ).invoke()
        return NCall(
            self.client.secrets.kv.v2.create_or_update_secret,
            {"path": id, "secret": dict({self.value_field: value})},
            nargs,
        ).invoke()

    def update_metadata(self, id: str, metadata: dict, nargs: Any) -> Any:
        NCall(
            self.client.secrets.kv.read_secret,
            {"path": id, "raise_on_deleted_version": True},
            nargs,
            {InvalidPath: NotFoundError},
        ).invoke()
        return NCall(
            self.client.secrets.kv.v2.update_metadata,
            {"path": id, "custom_metadata": metadata},
            nargs,
        ).invoke()

    def delete(self, id: str, nargs: Any) -> Any:
        NCall(
            self.client.secrets.kv.read_secret,
            {"path": id, "raise_on_deleted_version": True},
            nargs,
            {InvalidPath: NotFoundError},
        ).invoke()
        return NCall(
            self.client.secrets.kv.v2.delete_metadata_and_all_versions,
            {"path": id},
            nargs,
        ).invoke()

    def close(self, nargs: Any) -> Any:
        pass
