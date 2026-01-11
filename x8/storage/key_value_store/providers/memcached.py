"""
Key Value Store on Memcached.
"""

from __future__ import annotations

__all__ = ["Memcached"]

import re
from typing import Any
from urllib.parse import unquote

from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import Expression, QueryProcessor, Update, UpdateOp
from x8.storage._common import (
    Attribute,
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    UpdateAttribute,
)

from .._helper import (
    build_item,
    convert_value,
    get_collection_key,
    get_collection_name,
    get_collection_prefix,
)
from .._models import (
    KeyValueItem,
    KeyValueKeyType,
    KeyValueList,
    KeyValueQueryConfig,
    KeyValueValueType,
)


class Memcached(StoreProvider):
    host: str
    port: int
    collection: str | None
    nparams: dict[str, Any]

    _lib: Any
    _alib: Any

    _client: Any
    _aclient: Any
    _item_processor: ItemProcessor
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        host: str = "localhost",
        port: int = 11211,
        collection: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            host:
                Memcached host.
            port:
                Memcached port.
            collection:
                Collection name.
            nparams:
                Native parameters to the redis client.
        """
        self.host = host
        self.port = port
        self.collection = collection
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._item_processor = ItemProcessor()
        self._op_converter = OperationConverter(processor=self._item_processor)

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from pymemcache.client import Client

        options: dict = self.nparams if self.nparams is not None else {}
        self._client = Client(
            (self.host, self.port),
            default_noreply=False,
            **options,
        )
        self._result_converter = ResultConverter(
            self._item_processor, self.__component__.type
        )

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall, state = self._get_ncall(
            self._client,
            op_parser,
            ClientHelper(self._client, self._op_converter),
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(
            nresult,
            state,
            op_parser,
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        client: Any,
        op_parser: StoreOperationParser,
        client_helper: Any,
    ) -> tuple[NCall | None, dict | None]:
        call = None
        state = None
        nargs = op_parser.get_nargs()
        collection: str | None = get_collection_name(self, op_parser)
        op_converter = self._op_converter
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            args: Any = op_converter.convert_exists(
                op_parser.get_key(),
                collection,
            )
            call = NCall(client.gets, args, nargs)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = op_converter.convert_get(
                op_parser.get_key(),
                op_parser.get_start(),
                op_parser.get_end(),
                collection,
            )
            call = NCall(client.gets, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, func = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
                op_parser.get_expiry_in_seconds(),
                op_parser.get_returning(),
                collection,
            )
            if func == "set":
                func_ref = client.set
            elif func == "add":
                func_ref = client.add
            elif func == "replace":
                func_ref = client.replace
            elif func == "cas":
                func_ref = client.cas
            call = NCall(
                func_ref,
                args,
                nargs,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args = {
                "key": op_parser.get_key(),
                "set": op_parser.get_set(),
                "where_exists": op_parser.get_where_exists(),
                "where_etag": op_parser.get_where_etag(),
                "returning": op_parser.get_returning(),
                "collection": collection,
                "nargs": nargs,
            }
            call = NCall(client_helper.update, args)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = op_converter.convert_delete(
                op_parser.get_key(),
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
                collection,
            )
            call = NCall(client.delete, args, nargs)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = {"nargs": nargs}
            call = NCall(client_helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = {"nargs": nargs}
            call = NCall(client_helper.query, args)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, func = op_converter.convert_batch(
                op_parser.get_operation_parsers(),
                collection,
            )
            if func == "set_multi":
                func_ref = client.set_multi
            elif func == "get_multi":
                func_ref = client.get_multi
            elif func == "helper.batch_delete":
                func_ref = client_helper.batch_delete
            state = {"func": func}
            call = NCall(func_ref, args, nargs)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(client_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict | None,
        op_parser: StoreOperationParser,
    ) -> Any:
        result_converter = self._result_converter
        result: Any = None
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            result = result_converter.convert_exists(nresult)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            result = result_converter.convert_get(
                nresult,
                op_parser.get_key(),
                op_parser.get_start(),
                op_parser.get_end(),
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = result_converter.convert_put(
                nresult,
                op_parser.get_key(),
                op_parser.get_returning(),
                op_parser.get_value(),
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(
                nresult,
                op_parser.get_key(),
                op_parser.get_returning(),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = result_converter.convert_delete(nresult)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            collection = get_collection_name(self, op_parser)
            result = result_converter.convert_query(
                nresult,
                op_parser.get_where(),
                op_parser.get_limit(),
                collection,
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            collection = get_collection_name(self, op_parser)
            result = result_converter.convert_count(
                nresult,
                op_parser.get_where(),
                op_parser.get_limit(),
                collection,
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            func = state["func"] if state else "mset"
            collection = get_collection_name(self, op_parser)
            result = result_converter.convert_batch(
                nresult,
                func,
                op_parser.get_operation_parsers(),
                collection,
            )
        return result


class ClientHelper:
    client: Any
    op_converter: OperationConverter

    def __init__(self, client: Any, op_converter: OperationConverter):
        self.client = client
        self.op_converter = op_converter

    def update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
        nargs: Any,
    ) -> Any:
        args, func = self.op_converter.convert_update(
            key, set, where_exists, where_etag, returning, collection
        )
        if func == "incr":
            func_ref = self.client.incr
        elif func == "decr":
            func_ref = self.client.decr
        elif func == "append":
            func_ref = self.client.append
        elif func == "prepend":
            func_ref = self.client.prepend
        response = NCall(func_ref, args, nargs).invoke()
        if response is not None:
            return response
        response = NCall(self.client.set, args, nargs).invoke()
        return args["value"]

    def query(self, nargs: Any) -> str:
        response = NCall(
            self.client.raw_command,
            {"command": b"lru_crawler metadump all\r\n"},
            nargs,
        ).invoke()
        self.client.quit()
        return response

    def batch_delete(self, keys: list) -> None:
        for key in keys:
            self.client.delete(key=key)

    def close(self, nargs: Any) -> Any:
        pass


class OperationConverter:
    processor: ItemProcessor

    def __init__(self, processor: ItemProcessor):
        self.processor = processor

    def convert_exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return {"key": db_key}

    def convert_get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return {"key": db_key}

    def convert_put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: str | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
    ) -> tuple[dict, str]:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        expire = int(expiry) if expiry else 0
        args: dict = {"key": db_key, "value": value, "expire": expire}
        if where_exists is False:
            return args, "add"
        elif where_exists is True:
            return args, "replace"
        elif where_etag:
            args["cas"] = where_etag
            return args, "cas"
        return args, "set"

    def convert_update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
    ) -> tuple[dict, str]:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)

        if len(set.operations) == 1 and (
            set.operations[0].field == Attribute.VALUE
            or set.operations[0].field == UpdateAttribute.VALUE
        ):
            op = set.operations[0].op
            if op == UpdateOp.INCREMENT:
                amount = set.operations[0].args[0]
                if amount >= 0:
                    args = {"key": db_key, "value": amount}
                    return args, "incr"
                else:
                    args = {"key": db_key, "value": -amount}
                    return args, "decr"
            elif op == UpdateOp.APPEND:
                value = set.operations[0].args[0]
                args = {"key": db_key, "value": value}
                return args, "append"
            elif op == UpdateOp.PREPEND:
                value = set.operations[0].args[0]
                args = {"key": db_key, "value": value}
                return args, "prepend"
        raise BadRequestError(f"Update operation not supported: {set}")

    def convert_delete(
        self,
        key: KeyValueKeyType,
        where_exists: bool | None,
        where_etag: bool | None,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return {"key": db_key}

    def convert_query(
        self,
        where: Expression | None,
        limit: int | None,
        continuation: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
    ) -> dict:
        return {"command": b"lru_crawler metadump all\r\n"}

    def convert_count(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> dict:
        return {"command": b"lru_crawler metadump all\r\n"}

    def convert_batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
    ) -> tuple[dict | list, str]:
        def is_batch_op(
            op_parsers: list[StoreOperationParser], op: str
        ) -> bool:
            for op_parser in op_parsers:
                if not op_parser.op_equals(op):
                    return False
            return True

        if is_batch_op(op_parsers, StoreOperation.PUT):
            kvs = {}
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                id = self.processor.get_id_from_key(op_parser.get_key())
                db_key = get_collection_key(col, id)
                value = op_parser.get_value()
                kvs[db_key] = value
            return {"values": kvs}, "set_multi"
        elif is_batch_op(op_parsers, StoreOperation.GET):
            keys = []
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                id = self.processor.get_id_from_key(op_parser.get_key())
                db_key = get_collection_key(col, id)
                keys.append(db_key)
            return {"keys": keys}, "get_multi"
        elif is_batch_op(op_parsers, StoreOperation.DELETE):
            keys = []
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                id = self.processor.get_id_from_key(op_parser.get_key())
                db_key = get_collection_key(col, id)
                keys.append(db_key)
            return {"keys": keys}, "helper.batch_delete"
        else:
            raise BadRequestError("Batch operations not supported")


class ResultConverter:
    processor: ItemProcessor
    type: str

    def __init__(self, processor: ItemProcessor, type: str):
        self.processor = processor
        self.type = type

    def convert_exists(self, nresult: Any) -> bool:
        _, cas = nresult
        if cas is None:
            return False
        return True

    def convert_get(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
    ) -> KeyValueItem:
        value, cas = nresult
        if cas is None:
            raise NotFoundError
        id = self.processor.get_id_from_key(key)
        if self.type == "string":
            value = value.decode()
        if start and end:
            eend = end + 1
            value = value[start:eend]
        elif start:
            value = value[start:]
        elif end:
            eend = end + 1
            value = value[:eend]
        return build_item(id, value, etag=cas.decode())

    def convert_put(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
        value: KeyValueValueType,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if not nresult:
            raise PreconditionFailedError
        if returning == "new":
            return build_item(id, convert_value(value, self.type))
        return build_item(id)

    def convert_update(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        return_value: Any = None
        if returning == "new":
            if isinstance(nresult, bytes):
                if self.type == "string":
                    return_value = nresult.decode()
                else:
                    return_value = nresult
            else:
                return_value = nresult
        return build_item(id, convert_value(return_value, self.type))

    def convert_delete(self, nresult: Any):
        if not nresult:
            raise NotFoundError
        return None

    def convert_query(
        self,
        nresult: Any,
        where: Expression | None,
        limit: int | None,
        collection: str | None,
    ):
        filtered_items = self._parse_keys(nresult, where, limit, collection)
        items = [build_item(id=fitem["key"]["id"]) for fitem in filtered_items]
        return KeyValueList(items=items)

    def convert_count(
        self,
        nresult: Any,
        where: Expression | None,
        limit: int | None,
        collection: str | None,
    ):
        filtered_items = self._parse_keys(nresult, where, limit, collection)
        return len(filtered_items)

    def _parse_keys(
        self,
        nresult: Any,
        where: Expression | None,
        limit: int | None,
        collection: str | None,
    ) -> list:
        response = nresult.decode()
        keys = re.findall(r"key=([^ ]+)", response)
        keys = [unquote(key) for key in keys]
        collection_prefix = get_collection_prefix(collection)
        start = len(collection_prefix) if collection_prefix else 0
        filtered_items = [
            {"key": {"id": key[start:]}}
            for key in keys
            if key.startswith(collection_prefix)
        ]
        filtered_items = QueryProcessor.query_items(
            items=filtered_items,
            where=where,
            limit=limit,
            field_resolver=self.processor.resolve_root_field,
        )
        return filtered_items

    def convert_batch(
        self,
        nresult: Any,
        func: str,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
    ):
        result: list = []
        if func == "get_multi":
            for op_parser in op_parsers:
                key = op_parser.get_key()
                id = self.processor.get_id_from_key(key)
                col = op_parser.get_collection_name() or collection
                db_key = get_collection_key(col, id)
                value = nresult[db_key]
                if self.type == "string":
                    value = value.decode()
                result.append(build_item(id=id, value=value))
        elif func == "set_multi":
            for op_parser in op_parsers:
                key = op_parser.get_key()
                id = self.processor.get_id_from_key(key)
                result.append(build_item(id=id))
        return result
