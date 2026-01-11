"""
Config Store on Azure App Configuration.
"""

__all__ = ["AzureAppConfiguration"]

from typing import Any

from azure.appconfiguration import ConfigurationSetting
from azure.core.exceptions import ResourceNotFoundError
from x8._common.azure_provider import AzureProvider
from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import BadRequestError, NotFoundError
from x8.storage._common import (
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)

from .._helper import QueryArgs, get_query_args, normalize_label
from .._models import ConfigItem, ConfigKey, ConfigList, ConfigProperties


class AzureAppConfiguration(StoreProvider, AzureProvider):
    connection_string: str | None
    base_url: str | None
    nparams: dict[str, Any]

    _credential: Any
    _acredential: Any
    _client: Any
    _aclient: Any
    _processor: ItemProcessor

    def __init__(
        self,
        connection_string: str | None = None,
        base_url: str | None = None,
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
            connection_string:
                Azure App Configuration connection string.
                If connection string is provided,
                base_url is optional and ignored.
            base_url:
                Azure App Configuration base url.
                If connection_string is not provided, base_url is required.
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
                Native parameters to Azure App Configuration client.
        """
        self.connection_string = connection_string
        self.base_url = base_url
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

        from azure.appconfiguration import AzureAppConfigurationClient

        credential = None
        if self.base_url is not None:
            credential = self._get_credential()
            client = AzureAppConfigurationClient(
                base_url=self.base_url,
                credential=credential,
                **self.nparams,
            )
        elif self.connection_string is not None:
            client = AzureAppConfigurationClient.from_connection_string(
                self.connection_string, **self.nparams
            )
        else:
            raise BadRequestError(
                "Either connection_string or base_url is required."
            )

        self._credential, self._client = credential, client

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        from azure.appconfiguration.aio import AzureAppConfigurationClient

        credential = None
        if self.base_url is not None:
            credential = self._aget_credential()
            client = AzureAppConfigurationClient(
                base_url=self.base_url,
                credential=credential,
                **self.nparams,
            )
        elif self.connection_string is not None:
            client = AzureAppConfigurationClient.from_connection_string(
                self.connection_string, **self.nparams
            )
        else:
            raise BadRequestError(
                "Either connection_string or base_url is required."
            )

        self._acredential, self._aclient = credential, client

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

        # GET
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            args: dict[str, Any] = {"key": id, "label": label}
            call = NCall(
                client.get_configuration_setting,
                args,
                nargs,
                {ResourceNotFoundError: NotFoundError},
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            label = normalize_label(op_parser.get_label())
            config = ConfigurationSetting(
                key=id,
                label=label,
                value=value,
            )
            args = {"configuration_setting": config}
            call = NCall(client.set_configuration_setting, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            args = {"key": id, "label": label}
            call = NCall(
                client.delete_configuration_setting,
                args,
                nargs,
                {None: NotFoundError},
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = {
                "args": OperationConverter.convert_filter(
                    get_query_args(op_parser)
                ),
                "nargs": nargs,
            }
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = {
                "args": OperationConverter.convert_filter(
                    get_query_args(op_parser)
                ),
                "nargs": nargs,
            }
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

        def convert_item(item):
            return ConfigItem(
                key=ConfigKey(id=item.key, label=item.label),
                value=item.value,
                properties=ConfigProperties(
                    updated_time=(
                        item.last_modified.timestamp()
                        if item.last_modified
                        else None
                    )
                ),
            )

        # GET
        if op_parser.op_equals(StoreOperation.GET):
            result = convert_item(nresult)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = convert_item(nresult)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult:
                items.append(convert_item(item))
            items = sorted(items, key=lambda x: (x.key.label, x.key.id))
            result = ConfigList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            count = 0
            for item in nresult:
                count = count + 1
            result = count
        return result


class OperationConverter:
    @staticmethod
    def convert_filter(query_args: QueryArgs) -> dict:
        args = {}
        if query_args.id_filter is not None:
            args["key_filter"] = f"{query_args.id_filter}*"
        if query_args.label_filter is not None:
            args["label_filter"] = query_args.label_filter
        return args


class ClientHelper:
    client: Any
    credential: Any

    def __init__(self, client: Any, credential: Any):
        self.client = client
        self.credential = credential

    def query(self, args: dict, nargs: Any):
        return NCall(
            self.client.list_configuration_settings,
            args,
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

    async def query(self, args: dict, nargs: Any):
        response = NCall(
            self.client.list_configuration_settings,
            args,
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
        if self.credential is not None:
            await NCall(
                self.credential.close,
                None,
                nargs,
            ).ainvoke()
