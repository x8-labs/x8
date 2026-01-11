"""
Config Store on AWS Parameter Store.
"""

__all__ = ["AWSParameterStore"]

from typing import Any

import boto3
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


class AWSParameterStore(StoreProvider):
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    tier: str
    nparams: dict[str, Any]

    _client: Any
    _processor: ItemProcessor

    def __init__(
        self,
        region: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        tier: str = "Standard",
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            region_name:
                AWS region name.
            profile_name:
                AWS profile name.
            aws_access_key_id:
                AWS access key id.
            aws_secret_access_key:
                AWS secret access key.
            aws_session_token:
                AWS session token.
            tier:
                AWS Parameter Store storage tier, defaults to "Standard".
            nparams:
                Native parameters to boto3 client.
        """
        self.region = region
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.tier = tier
        self.nparams = nparams

        self._client = None
        self._processor = ItemProcessor()

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        session = None
        if self.profile_name is not None:
            session = boto3.session.Session(
                profile_name=self.profile_name,
                **self.nparams,
            )
        elif (
            self.aws_access_key_id is not None
            and self.aws_secret_access_key is not None
        ):
            session = boto3.session.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                **self.nparams,
            )
        else:
            session = boto3.session.Session(
                **self.nparams,
            )

        self._client = session.client(
            service_name="ssm",
            region_name=self.region,
            **self.nparams,
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
            ClientHelper(self._client, self.tier),
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
            args = {
                "id": id,
                "label": label,
                "nargs": nargs,
            }
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
            args = {"id": id, "label": label, "nargs": nargs}
            call = NCall(helper.delete, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = {"query_args": get_query_args(op_parser), "nargs": nargs}
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = {"query_args": get_query_args(op_parser), "nargs": nargs}
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
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            result = ConfigItem(
                key=ConfigKey(
                    id=nresult["Parameter"]["Name"],
                    label=nresult["Parameter"]["Selector"][1:],
                ),
                value=nresult["Parameter"]["Value"],
                properties=ConfigProperties(
                    updated_time=(
                        nresult["Parameter"]["LastModifiedDate"].timestamp()
                        if nresult["Parameter"]["LastModifiedDate"]
                        else None
                    ),
                ),
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            label = normalize_label(op_parser.get_label())
            result = ConfigItem(
                key=ConfigKey(id=id, label=label),
                value=value,
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult:
                if len(item["Labels"]) > 0:
                    items.append(
                        ConfigItem(
                            key=ConfigKey(
                                id=item["Name"], label=item["Labels"][0]
                            ),
                            value=item["Value"],
                            properties=ConfigProperties(
                                updated_time=(
                                    item["LastModifiedDate"].timestamp()
                                    if item["LastModifiedDate"]
                                    else None
                                ),
                            ),
                        )
                    )
            items = sorted(items, key=lambda x: (x.key.label, x.key.id))
            result = ConfigList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            count = 0
            for item in nresult:
                if len(item["Labels"]) > 0:
                    count = count + 1
            result = count
        return result


class OperationConverter:
    pass


class ClientHelper:
    client: Any
    tier: str

    def __init__(self, client: Any, tier: str):
        self.client = client
        self.tier = tier

    def get(self, id: str, label: str, nargs: Any) -> Any:
        ex = self.client.exceptions
        args: dict[str, Any] = {"Name": f"{id}:{label}"}
        return NCall(
            self.client.get_parameter,
            args,
            nargs,
            {
                ex.ParameterNotFound: NotFoundError,
                ex.ParameterVersionNotFound: NotFoundError,
            },
        ).invoke()

    def put(
        self,
        id: str,
        label: str,
        value: str,
        nargs: Any,
    ) -> Any:
        args = {
            "Name": id,
            "Value": value,
            "Type": "String",
            "Overwrite": True,
            "Tier": self.tier,
        }
        res = NCall(self.client.put_parameter, args, nargs).invoke()
        version = res["Version"]
        args = {"Name": id, "ParameterVersion": version, "Labels": [label]}
        NCall(self.client.label_parameter_version, args, nargs).invoke()
        return res

    def delete(self, id: str, label: str, nargs: Any) -> Any:
        res = self.get(id, label, nargs)
        version = res["Parameter"]["Version"]
        res = NCall(
            self.client.unlabel_parameter_version,
            {
                "Name": id,
                "ParameterVersion": version,
                "Labels": [label],
            },
            nargs,
        ).invoke()
        if len(res["InvalidLabels"]) > 0:
            raise BadRequestError("Could not delete. Try again!")

        # Removing the label is async.
        # Check if no versions have labels other than
        # the current label and delete parameter.
        no_labels = True
        versions = self._list_versions(id, nargs)
        for item in versions:
            if len(item["Labels"]) == 1:
                if item["Labels"][0] == label and item["Version"] == version:
                    pass
                else:
                    no_labels = False
            elif len(item["Labels"]) > 1:
                no_labels = False
        if no_labels:
            res = NCall(
                self.client.delete_parameter,
                {"Name": id},
                nargs,
            ).invoke()
        return None

    def query(self, query_args: QueryArgs, nargs: Any) -> Any:
        return self._query_parameters(
            query_args.id_filter, query_args.label_filter, nargs
        )

    def close(self, nargs: Any) -> Any:
        pass

    def _query_parameters(
        self, id_filter: str | None, label_filter: str | None, nargs: Any
    ) -> Any:
        parameters = []
        next_token = None
        while True:
            args = None
            if id_filter:
                args = {
                    "ParameterFilters": [
                        {
                            "Key": "Name",
                            "Option": "BeginsWith",
                            "Values": [id_filter],
                        }
                    ]
                }
            if next_token is not None:
                if args is None:
                    args = {"NextToken": next_token}
                else:
                    args = args | {"NextToken": next_token}
            res = NCall(
                self.client.describe_parameters,
                args,
                nargs,
            ).invoke()
            parameters.extend(res["Parameters"])
            if "NextToken" in res and res["NextToken"] is not None:
                next_token = res["NextToken"]
            else:
                next_token = None
            if next_token is None:
                break
        result = []
        for parameter in parameters:
            versions = self._list_versions(parameter["Name"], nargs)
            if label_filter is not None:
                for item in versions:
                    if label_filter in item["Labels"]:
                        result.append(item)
            else:
                result.extend(versions)
        return result

    def _list_versions(self, id: str, nargs: Any) -> Any:
        versions = []
        next_token = None
        while True:
            args = {"Name": id}
            if next_token is not None:
                args["NextToken"] = next_token
            res = NCall(
                self.client.get_parameter_history,
                args,
                nargs,
            ).invoke()
            versions.extend(res["Parameters"])
            if "NextToken" in res and res["NextToken"] is not None:
                next_token = res["NextToken"]
            else:
                next_token = None
            if next_token is None:
                break
        return versions

    # get_parameters_by_path has strict begins with semantics.
    # not using for now.
    def _list(self, query_args: QueryArgs, nargs: Any):
        parameters = []
        id_filter = query_args.id_filter
        label_filter = query_args.label_filter
        next_token = None
        while True:
            args: dict = {"Path": id_filter}
            if label_filter is not None:
                args = args | {
                    "ParameterFilters": {
                        "Key": "Label",
                        "Values": [label_filter],
                    }
                }
            if next_token is not None:
                args["NextToken"] = next_token
            res = NCall(
                self.client.get_parameters_by_path,
                args,
                nargs,
            ).invoke()
            parameters.extend(res["Parameters"])
            if "NextToken" in res and res["NextToken"] is not None:
                next_token = res["NextToken"]
            else:
                next_token = None
            if next_token is None:
                break
        return parameters
