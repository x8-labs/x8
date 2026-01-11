"""
Secret Store on AWS Secrets Manager.
"""

__all__ = ["AWSSecretsManager"]

from typing import Any

import boto3

from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import NotFoundError, PreconditionFailedError
from x8.ql import OrderBy, OrderByTerm, QueryProcessor
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


class AWSSecretsManager(StoreProvider):
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
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
            nparams:
                Native parameters to boto3 client.
        """
        self.region = region
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
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
            session = boto3.session.Session(**self.nparams)

        self._client = session.client(
            service_name="secretsmanager",
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
            ClientHelper(self._client),
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
        ex = client.exceptions
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            args: dict[str, Any] = {"SecretId": id}
            if version is not None:
                args["VersionId"] = version
            call = NCall(
                client.get_secret_value,
                args,
                nargs,
                {
                    ex.ResourceNotFoundException: NotFoundError,
                    ex.InvalidRequestException: NotFoundError,
                },
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            args = {"SecretId": id}
            call = NCall(
                client.describe_secret,
                args,
                nargs,
                {ex.ResourceNotFoundException: NotFoundError},
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            args = {"SecretId": id, "IncludeDeprecated": True}
            call = NCall(
                client.list_secret_version_ids,
                args,
                nargs,
                {ex.ResourceNotFoundException: NotFoundError},
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            metadata = op_parser.get_metadata()
            exists = op_parser.get_where_exists()
            args = {
                "Name": id,
                "SecretString": value,
                "Tags": Helper.convert_metadata_to_list(metadata),
            }
            if exists is False:
                call = NCall(
                    client.create_secret,
                    args,
                    nargs,
                    {ex.ResourceExistsException: PreconditionFailedError},
                )
            elif exists is None or exists is True:
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
            args = {
                "SecretId": id,
                "SecretString": value,
            }
            call = NCall(
                client.update_secret,
                args,
                nargs,
                {ex.ResourceNotFoundException: NotFoundError},
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
                    id=nresult["Name"], version=nresult["VersionId"]
                ),
                value=nresult["SecretString"],
            )
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            raise_on_deleted = True
            nflags = op_parser.get_nflags()
            if (
                nflags is not None
                and "raise_on_deleted" in nflags
                and nflags["raise_on_deleted"] is False
            ):
                raise_on_deleted = False
            if "DeletedDate" in nresult:
                if raise_on_deleted:
                    raise NotFoundError
            result = SecretItem(
                key=SecretKey(id=nresult["Name"]),
                metadata=Helper.convert_list_to_metadata(nresult["Tags"]),
                properties=SecretProperties(
                    created_time=nresult["CreatedDate"].timestamp()
                ),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            versions: list = []
            for item in nresult["Versions"]:
                versions.append(
                    SecretVersion(
                        version=item["VersionId"],
                        created_time=item["CreatedDate"].timestamp(),
                    )
                )
            versions.reverse()
            return SecretItem(key=SecretKey(id=id), versions=versions)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = SecretItem(
                key=SecretKey(id=nresult["Name"], version=nresult["VersionId"])
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = SecretItem(
                key=SecretKey(id=nresult["Name"], version=nresult["VersionId"])
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
            for response in nresult:
                for item in response["SecretList"]:
                    if "DeletedDate" in item:
                        continue
                    items.append(
                        SecretItem(
                            key=SecretKey(id=item["Name"]),
                            metadata=Helper.convert_list_to_metadata(
                                item["Tags"]
                            ),
                            properties=SecretProperties(
                                created_time=item["CreatedDate"].timestamp()
                            ),
                        )
                    )
            order_by = (
                op_parser.get_order_by()
                if op_parser.get_order_by() is not None
                else OrderBy(terms=[OrderByTerm(field="$id")])
            )
            items = QueryProcessor.query_items(
                items=items,
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=order_by,
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
                field_resolver=processor.resolve_root_field,
            )
            result = SecretList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            items = []
            for response in nresult:
                for item in response["SecretList"]:
                    if "DeletedDate" in item:
                        continue
                    items.append(
                        SecretItem(
                            key=SecretKey(id=item["Name"]),
                            metadata=Helper.convert_list_to_metadata(
                                item["Tags"]
                            ),
                            properties=SecretProperties(
                                created_time=item["CreatedDate"].timestamp()
                            ),
                        )
                    )
            result = QueryProcessor.count_items(
                items=items,
                where=op_parser.get_where(),
                field_resolver=processor.resolve_root_field,
            )
        return result


class Helper:
    @staticmethod
    def convert_metadata_to_list(metadata: dict | None) -> list[dict]:
        if metadata is None:
            return []
        metadata_list = []
        for key, value in metadata.items():
            metadata_list.append({"Key": key, "Value": value})
        return metadata_list

    @staticmethod
    def convert_list_to_metadata(metadata_list: list[Any]) -> dict:
        if not metadata_list:
            return {}
        metadata: dict = {}
        for item in metadata_list:
            metadata[item["Key"]] = item["Value"]
        return metadata


class ClientHelper:
    client: Any

    def __init__(self, client: Any):
        self.client = client

    def put(
        self,
        id: str,
        value: str,
        metadata: dict | None,
        where_exists: bool | None,
        nargs: Any,
    ) -> Any:
        ex = self.client.exceptions
        if where_exists is None:
            (response, error) = NCall(
                self.client.create_secret,
                {
                    "Name": id,
                    "SecretString": value,
                    "Tags": Helper.convert_metadata_to_list(metadata),
                },
                nargs,
                {ex.ResourceExistsException: None},
            ).invoke(return_error=True)
            if not error:
                return response
        response = NCall(
            self.client.update_secret,
            {"SecretId": id, "SecretString": value},
            nargs,
            {
                ex.ResourceNotFoundException: (
                    PreconditionFailedError
                    if where_exists is True
                    else NotFoundError
                )
            },
        ).invoke()
        self.update_metadata(id, metadata, nargs)
        return response

    def update_metadata(
        self, id: str, metadata: dict | None, nargs: Any
    ) -> Any:
        ex = self.client.exceptions
        response = NCall(
            self.client.describe_secret,
            {"SecretId": id},
            nargs,
            {ex.ResourceNotFoundException: NotFoundError},
        ).invoke()
        prev_metadata = [o["Key"] for o in response["Tags"]]
        NCall(
            self.client.untag_resource,
            {"SecretId": id, "TagKeys": prev_metadata},
            nargs,
            {ex.ResourceNotFoundException: NotFoundError},
        ).invoke()
        if metadata is not None:
            NCall(
                self.client.tag_resource,
                {
                    "SecretId": id,
                    "Tags": Helper.convert_metadata_to_list(metadata),
                },
                nargs,
                {ex.ResourceNotFoundException: NotFoundError},
            ).invoke()

    def delete(self, id: str, nargs: Any) -> Any:
        response = NCall(
            self.client.describe_secret,
            {"SecretId": id},
            nargs,
            {self.client.exceptions.ResourceNotFoundException: NotFoundError},
        ).invoke()
        if "DeletedDate" in response:
            raise NotFoundError
        NCall(
            self.client.delete_secret,
            {"SecretId": id, "ForceDeleteWithoutRecovery": True},
            nargs,
        ).invoke()

    def query(self, nargs: Any):
        responses = []
        next_token = None
        while True:
            args = {
                "MaxResults": 100,
                "Filters": [],
                "IncludePlannedDeletion": False,
            }
            if next_token is not None:
                args["NextToken"] = next_token
            response = NCall(self.client.list_secrets, args, nargs).invoke()
            responses.append(response)
            if "NextToken" in response:
                next_token = response["NextToken"]
            else:
                break
        return responses

    def close(self, nargs: Any) -> Any:
        pass
