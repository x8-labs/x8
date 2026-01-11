"""
Config Store on Google Runtime Configurator.
"""

__all__ = ["GoogleRuntimeConfigurator"]

from datetime import datetime
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from x8._common.google_provider import GoogleProvider
from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import NotFoundError
from x8.storage._common import (
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)

from .._helper import QueryArgs, get_query_args, normalize_label
from .._models import ConfigItem, ConfigKey, ConfigList, ConfigProperties


class GoogleRuntimeConfigurator(StoreProvider, GoogleProvider):
    project: str | None
    delete_empty_config: bool
    nparams: dict[str, Any]

    _credentials: Any
    _client: Any
    _processor: ItemProcessor

    def __init__(
        self,
        project: str | None = None,
        delete_empty_config: bool = False,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            project:
                Google project name.
            delete_empty_config:
                A value indicating whether empty configs with no variables
                should be deleted on a delete operation., defaults to False.
            service_account_info:
                Google service account info with serialized credentials.
            service_account_file:
                Google service account file with credentials.
            access_token:
                Google access token.
            nparams:
                Native params to Google client. Not currently used.
        """
        self.project = project
        self.delete_empty_config = delete_empty_config
        self.nparams = nparams

        self._credentials = None
        self._client = None
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

        if self._credentials is not None:
            self._credentials = self._get_credentials()

        if self._credentials is not None:
            self._client = build(
                "runtimeconfig", "v1beta1", credentials=self._credentials
            )
        else:
            self._client = build("runtimeconfig", "v1beta1")

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
            args: dict[str, Any] = {"id": id, "label": label, "nargs": nargs}
            call = NCall(helper.get, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            label = normalize_label(op_parser.get_label())
            args = {
                "id": id,
                "label": label,
                "value": value,
                "nargs": nargs,
            }
            call = NCall(helper.put, args)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            args = {
                "id": id,
                "label": label,
                "delete_empty_config": self.delete_empty_config,
                "nargs": nargs,
            }
            call = NCall(helper.delete, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            query_args = get_query_args(op_parser)
            args = {"query_args": query_args, "nargs": nargs}
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            query_args = get_query_args(op_parser)
            args = {"query_args": query_args, "nargs": nargs}
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

        def extract_id_label(path) -> tuple[str, str]:
            splits = path.split("/")
            id = "/".join(splits[5:])
            if "/" in id and id[0] != "/":
                id = f"/{id}"
            return id, splits[3]

        # GET
        if op_parser.op_equals(StoreOperation.GET):
            id, label = extract_id_label(nresult["name"])
            result = ConfigItem(
                key=ConfigKey(id=id, label=label),
                value=nresult["text"],
                properties=ConfigProperties(
                    updated_time=Helper.convert_to_timestamp(
                        nresult["updateTime"]
                    )
                ),
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id, label = extract_id_label(nresult["name"])
            result = ConfigItem(
                key=ConfigKey(id=id, label=label),
                value=op_parser.get_value(),
                properties=ConfigProperties(
                    updated_time=Helper.convert_to_timestamp(
                        nresult["updateTime"]
                    )
                ),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult:
                id, label = extract_id_label(item["name"])
                items.append(
                    ConfigItem(
                        key=ConfigKey(id=id, label=label),
                        value=item["text"],
                        properties=ConfigProperties(
                            updated_time=Helper.convert_to_timestamp(
                                item["updateTime"]
                            )
                        ),
                    )
                )
            items = sorted(items, key=lambda x: (x.key.label, x.key.id))
            result = ConfigList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = len(nresult)
        return result


class OperationConverter:
    @staticmethod
    def convert_filter(
        query_args: QueryArgs,
    ) -> tuple[str | None, str | None]:
        id_filter = None
        label_filter = None
        if query_args.id_filter is not None:
            if query_args.id_filter.startswith("/"):
                id_filter = query_args.id_filter.lstrip("/")
            else:
                id_filter = query_args.id_filter
        label_filter = query_args.label_filter
        return id_filter, label_filter


class Helper:
    @staticmethod
    def convert_to_timestamp(timestamp):
        return datetime.fromisoformat(timestamp).timestamp()


class ClientHelper:
    client: Any
    project: str

    def __init__(self, client: Any, project: str):
        self.client = client
        self.project = project

    def get(self, id: str, label: str, nargs: Any):
        name = f"projects/{self.project}/configs/{label}/variables/{id}"
        try:
            return NCall(
                self.client.projects()
                .configs()
                .variables()
                .get(name=name)
                .execute,
                None,
            ).invoke()
        except HttpError as e:
            if e.status_code == 404:
                raise NotFoundError
            raise

    def put(self, id: str, label: str, value: str, nargs: Any) -> Any:
        parent = f"projects/{self.project}/configs/{label}"
        name = f"{parent}/variables/{id}"
        body = {"name": name, "text": value}
        try:
            return NCall(
                self.client.projects()
                .configs()
                .variables()
                .create(parent=parent, body=body)
                .execute,
                None,
            ).invoke()
        except HttpError as e:
            if e.status_code == 404:
                config_body = {"name": parent}
                config_parent = f"projects/{self.project}"
                NCall(
                    self.client.projects()
                    .configs()
                    .create(parent=config_parent, body=config_body)
                    .execute,
                    None,
                ).invoke()
                return self.put(id, label, value, nargs)
            elif e.status_code == 409:
                return NCall(
                    self.client.projects()
                    .configs()
                    .variables()
                    .update(name=name, body=body)
                    .execute,
                    None,
                ).invoke()
            raise

    def delete(
        self,
        id: str,
        label: str,
        delete_empty_config: bool | None,
        nargs: Any,
    ) -> Any:
        parent = f"projects/{self.project}/configs/{label}"
        name = f"{parent}/variables/{id}"
        try:
            # Not sure why NCall wrapper doesn't work on delete calls
            self.client.projects().configs().variables().delete(
                name=name
            ).execute()
            if delete_empty_config:
                variables = self._list_variables(label, None, nargs)
                if len(variables) == 0:
                    self.client.projects().configs().delete(
                        name=parent
                    ).execute()
        except HttpError as e:
            if e.status_code == 404:
                raise NotFoundError
            raise

    def query(self, query_args: QueryArgs, nargs: Any):
        id_filter, label_filter = OperationConverter.convert_filter(query_args)
        if label_filter is None:
            variables = []
            configs = self._list_configs()
            for config in configs:
                label = config["name"].split("/")[3]
                id = id_filter
                if id_filter:
                    parent = f"projects/{self.project}/configs/{label}"
                    id = f"{parent}/variables/{id_filter}"
                variables.extend(
                    self._list_variables(
                        config["name"].split("/")[3], id, nargs
                    )
                )
            return variables
        else:
            if id_filter:
                parent = f"projects/{self.project}/configs/{label_filter}"
                id_filter = f"{parent}/variables/{id_filter}"
            return self._list_variables(label_filter, id_filter, nargs)

    def close(self, nargs: Any) -> Any:
        pass

    def _list_configs(self) -> Any:
        configs = []
        page_token = None
        parent = f"projects/{self.project}"

        while True:
            response = NCall(
                self.client.projects()
                .configs()
                .list(
                    parent=parent,
                    pageToken=page_token,
                )
                .execute,
                None,
            ).invoke()
            configs.extend(response["configs"])
            if (
                "nextPageToken" in response
                and response["nextPageToken"] is not None
            ):
                page_token = response["nextPageToken"]
            else:
                break
        return configs

    def _list_variables(
        self, label: str, id_filter: str | None, nargs: Any
    ) -> Any:
        variables = []
        page_token = None
        parent = f"projects/{self.project}/configs/{label}"

        while True:
            response = NCall(
                self.client.projects()
                .configs()
                .variables()
                .list(
                    parent=parent,
                    returnValues=True,
                    filter=id_filter,
                    pageToken=page_token,
                )
                .execute,
                None,
            ).invoke()
            if "variables" in response:
                variables.extend(response["variables"])
            if (
                "nextPageToken" in response
                and response["nextPageToken"] is not None
            ):
                page_token = response["nextPageToken"]
            else:
                break
        return variables
