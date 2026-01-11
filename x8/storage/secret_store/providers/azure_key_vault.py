"""
Secret Store on Azure Key Vault.
"""

__all__ = ["AzureKeyVault"]

from typing import Any

from azure.core.exceptions import ResourceNotFoundError

from x8._common.azure_provider import AzureProvider
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


class AzureKeyVault(AzureProvider, StoreProvider):
    vault_url: str
    nparams: dict[str, Any]

    _credential: Any
    _acredential: Any
    _client: Any
    _aclient: Any
    _processor: ItemProcessor

    def __init__(
        self,
        vault_url: str,
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
            vault_url:
                Azure Key Vault url.
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
                Native parameters to Azure Key Vault client.
        """
        self.vault_url = vault_url
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._processor = ItemProcessor()
        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from azure.keyvault.secrets import SecretClient

        credential = self._get_credential()
        client = SecretClient(vault_url=self.vault_url, credential=credential)
        self._credential, self._client = credential, client

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        from azure.keyvault.secrets.aio import SecretClient

        credential = self._get_credential()
        client = SecretClient(vault_url=self.vault_url, credential=credential)
        self._acredential, self._aclient = credential, client

    def _init_credential_client(
        self,
        secret_client,
        get_credential,
    ):
        credential = get_credential(
            credential_type=self.credential_type,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            certificate_path=self.certificate_path,
        )
        client = secret_client(vault_url=self.vault_url, credential=credential)
        return credential, client

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
            ClientHelper(self._client, self._credential),
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
            AsyncClientHelper(self._aclient, self._acredential),
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
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            args: dict[str, Any] = {"name": id, "version": version}
            call = NCall(
                client.get_secret,
                args,
                nargs,
                {ResourceNotFoundError: NotFoundError},
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            args = {"name": id}
            call = NCall(
                client.get_secret,
                args,
                nargs,
                {ResourceNotFoundError: NotFoundError},
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
            if exists is None:
                args = {
                    "name": id,
                    "value": value,
                    "tags": metadata if metadata is not None else {},
                }
                call = NCall(
                    client.set_secret,
                    args,
                    nargs,
                    {ResourceNotFoundError: NotFoundError},
                )
            elif exists is False or exists is True:
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
            call = NCall(helper.update, args)
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            id = op_parser.get_id_as_str()
            metadata = op_parser.get_metadata()
            args = {
                "name": id,
                "tags": metadata if metadata is not None else {},
            }
            call = NCall(
                client.update_secret_properties,
                args,
                nargs,
                {ResourceNotFoundError: NotFoundError},
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            args = {"id": id, "nargs": nargs}
            call = NCall(helper.delete, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = {"nargs": nargs}
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
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
            result = SecretItem(
                key=SecretKey(
                    id=nresult.name, version=nresult.properties.version
                ),
                value=nresult.value,
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            result = SecretItem(
                key=SecretKey(id=nresult.name),
                metadata=nresult.properties.tags,
                properties=SecretProperties(
                    created_time=nresult.properties.created_on.timestamp()
                ),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            versions: list = []
            for item in nresult:
                id = item.name
                versions.append(
                    SecretVersion(
                        version=item.version,
                        created_time=item.created_on.timestamp(),
                    )
                )
            if len(versions) == 0:
                raise NotFoundError
            versions = sorted(
                versions,
                key=lambda s: s.created_time,
                reverse=True,
            )
            return SecretItem(key=SecretKey(id=id), versions=versions)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = SecretItem(
                key=SecretKey(
                    id=nresult.name, version=nresult.properties.version
                )
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = SecretItem(
                key=SecretKey(
                    id=nresult.name, version=nresult.properties.version
                )
            )
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            result = SecretItem(
                key=SecretKey(id=nresult.name),
                metadata=nresult.tags,
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
                        key=SecretKey(id=item.name),
                        metadata=item.tags,
                        properties=SecretProperties(
                            created_time=item.created_on.timestamp()
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
                        key=SecretKey(id=item.name),
                        metadata=item.tags,
                        properties=SecretProperties(
                            created_time=item.created_on.timestamp()
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
    credential: Any

    def __init__(self, client: Any, credential: Any):
        self.client = client
        self.credential = credential

    def get_versions(self, id: str, nargs: Any):
        return NCall(
            self.client.list_properties_of_secret_versions,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).invoke()

    def put(
        self,
        id: str,
        value: str,
        metadata: dict | None,
        where_exists: bool,
        nargs: Any,
    ) -> Any:
        response = NCall(
            self.client.get_secret,
            {"name": id},
            nargs,
            {
                ResourceNotFoundError: (
                    None if not where_exists else PreconditionFailedError
                ),
            },
        ).invoke()
        if not where_exists and response is not None:
            raise PreconditionFailedError
        return NCall(
            self.client.set_secret,
            {"name": id, "value": value, "tags": metadata},
            nargs,
        ).invoke()

    def update(self, id: str, value: str, nargs: Any) -> Any:
        response = NCall(
            self.client.get_secret,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).invoke()
        return NCall(
            self.client.set_secret,
            {"name": id, "value": value, "tags": response.properties.tags},
            nargs,
        ).invoke()

    def delete(self, id: str, nargs: Any) -> Any:
        poller = NCall(
            self.client.begin_delete_secret,
            {"name": id},
            nargs,
            {
                # Secret could be in soft-delete state.
                # If secret doesn't exist, purge call will throw error.
                ResourceNotFoundError: None,
            },
        ).invoke()
        if poller is not None:
            poller.wait()
        NCall(
            self.client.purge_deleted_secret,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).invoke()

    def query(self, nargs: Any):
        return NCall(
            self.client.list_properties_of_secrets,
            None,
            nargs,
        ).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class AsyncClientHelper:
    client: Any
    credential: Any

    def __init__(self, client: Any, credential: Any):
        self.client = client
        self.credential = credential

    async def get_versions(self, id: str, nargs: Any):
        response = NCall(
            self.client.list_properties_of_secret_versions,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).invoke()
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
        response = await NCall(
            self.client.get_secret,
            {"name": id},
            nargs,
            {
                ResourceNotFoundError: (
                    None if not where_exists else PreconditionFailedError
                ),
            },
        ).ainvoke()
        if not where_exists and response is not None:
            raise PreconditionFailedError
        return await NCall(
            self.client.set_secret,
            {"name": id, "value": value, "tags": metadata},
            nargs,
        ).ainvoke()

    async def update(self, id: str, value: str, nargs: Any) -> Any:
        response = await NCall(
            self.client.get_secret,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).ainvoke()
        return await NCall(
            self.client.set_secret,
            {"name": id, "value": value, "tags": response.properties.tags},
            nargs,
        ).ainvoke()

    async def delete(self, id: str, nargs: Any):
        await NCall(
            self.client.delete_secret,
            {"name": id},
            nargs,
            {
                # Secret could be in soft-delete state.
                # If secret doesn't exist, purge call will throw error.
                ResourceNotFoundError: None,
            },
        ).ainvoke()
        await NCall(
            self.client.purge_deleted_secret,
            {"name": id},
            nargs,
            {ResourceNotFoundError: NotFoundError},
        ).ainvoke()

    async def query(self, nargs: Any):
        response = NCall(
            self.client.list_properties_of_secrets,
            None,
            nargs,
        ).invoke()
        nresult = []
        async for item in response:
            nresult.append(item)
        return nresult

    async def close(self, nargs: Any):
        await NCall(
            self.client.close,
            None,
            nargs,
        ).ainvoke()
        await NCall(
            self.credential.close,
            None,
            nargs,
        ).ainvoke()
