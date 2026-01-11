"""
Key Value Store on Redis.
"""

from __future__ import annotations

__all__ = ["RedisSimple"]

from typing import Any

from x8._common.redis_provider import RedisProvider
from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import (
    Comparison,
    ComparisonOp,
    Expression,
    Function,
    FunctionNamespace,
    QueryFunctionName,
    Update,
    UpdateOp,
)
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
    get_query_config,
)
from .._models import (
    KeyValueItem,
    KeyValueKeyType,
    KeyValueList,
    KeyValueQueryConfig,
    KeyValueValueType,
)


class RedisSimple(RedisProvider, StoreProvider):
    collection: str | None
    nparams: dict[str, Any]

    _lib: Any
    _alib: Any

    _client: Any
    _aclient: Any
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: str | int = 0,
        username: str | None = None,
        password: str | None = None,
        options: dict | None = None,
        collection: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            url:
                Redis endpoint url.
            host:
                Redis endpoint host name.
            port:
                Redis endpoint port.
            db:
                Redis database number.
            username:
                Username for auth.
            password:
                Password for auth.
            options:
                Redis client options.
            collection:
                Collection name.
            nparams:
                Native parameters to the redis client.
        """
        self.collection = collection
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._op_converter = OperationConverter(processor=ItemProcessor())
        self._result_converter = ResultConverter(processor=ItemProcessor())
        RedisProvider.__init__(
            self,
            url=url,
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            options=options,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        decode_responses = self.__component__.type != "binary"
        self._client, self._lib = self._get_client_and_lib(
            decode_responses=decode_responses,
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        decode_responses = self.__component__.type != "binary"
        self._aclient, self._alib = self._aget_client_and_lib(
            decode_responses=decode_responses,
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
            ClientHelper(self._client, self._lib),
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

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall, state = self._get_ncall(
            self._aclient,
            op_parser,
            AsyncClientHelper(self._aclient, self._alib),
        )
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
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
            call = NCall(client.exists, args, nargs)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args, func = op_converter.convert_get(
                op_parser.get_key(),
                op_parser.get_start(),
                op_parser.get_end(),
                collection,
            )
            if func == "get":
                call = NCall(client.get, args, nargs)
            elif func == "getrange":
                call = NCall(client.getrange, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where_exists(),
                op_parser.get_expiry(),
                op_parser.get_returning(),
                collection,
            )
            call = NCall(
                client.set,
                args,
                nargs,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, func = op_converter.convert_update(
                op_parser.get_key(),
                op_parser.get_set(),
                op_parser.get_where_exists(),
                op_parser.get_returning(),
                collection,
            )
            if func == "incrby":
                func_ref = client.incrby
            elif func == "incrbyfloat":
                func_ref = client.incrbyfloat
            elif func == "append":
                func_ref = client.append
            call = NCall(func_ref, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = op_converter.convert_delete(
                op_parser.get_key(),
                op_parser.get_where_exists(),
                collection,
            )
            call = NCall(client.delete, args, nargs)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = op_converter.convert_query(
                op_parser.get_where(),
                op_parser.get_limit(),
                op_parser.get_continuation(),
                get_query_config(op_parser),
                collection,
            )
            call = NCall(client_helper.query, args, nargs)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = op_converter.convert_count(
                op_parser.get_where(),
                collection,
            )
            args["nargs"] = nargs
            call = NCall(client_helper.count, args, nargs)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, func = op_converter.convert_batch(
                op_parser.get_operation_parsers(),
                collection,
            )
            if func == "mset":
                func_ref = client.mset
            elif func == "mget":
                func_ref = client.mget
            elif func == "delete":
                func_ref = client.delete
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
        type = self.__component__.type
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            result = result_converter.convert_exists(nresult)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            result = result_converter.convert_get(nresult, op_parser.get_key())
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = result_converter.convert_put(
                nresult,
                op_parser.get_key(),
                op_parser.get_returning(),
                op_parser.get_value(),
                type,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(
                nresult,
                op_parser.get_key(),
                op_parser.get_returning(),
                type,
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = result_converter.convert_delete(nresult)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            result = result_converter.convert_query(nresult)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = result_converter.convert_count(nresult)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            func = state["func"] if state else "mset"
            result = result_converter.convert_batch(
                nresult, func, op_parser.get_operation_parsers()
            )
        return result


class ClientHelper:
    client: Any
    lib: Any

    def __init__(self, client: Any, lib: Any):
        self.client = client
        self.lib = lib

    def query(
        self,
        continuation: str | None,
        limit: int | None,
        match: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
    ) -> KeyValueList:
        items: list = []
        prefix_length = len(collection) + 1 if collection else 0
        rcontinuation = continuation
        rlimit = limit
        if config and config.paging and config.page_size:
            if limit:
                if config.page_size < limit:
                    rlimit = config.page_size
            else:
                rlimit = config.page_size
        while True:
            cursor = int(rcontinuation) if rcontinuation else 0
            count = rlimit if rlimit else None
            cursor, keys = self.client.scan(
                cursor=cursor, count=count, match=match
            )
            for key in keys:
                items.append(build_item(id=key[prefix_length:]))
            if cursor == 0:
                rcontinuation = None
                break
            if config and config.paging and not config.page_size:
                rcontinuation = str(cursor)
                break
            if rlimit and len(items) < rlimit:
                rlimit = rlimit - len(items)
            rcontinuation = str(cursor)
        return KeyValueList(items=items, continuation=rcontinuation)

    def count(self, func: str, match: str | None, nargs: Any) -> int:
        count = 0
        if func == "dbsize":
            count = self.client.dbsize()
        elif func == "scan_iter":
            count = sum(1 for _ in self.client.scan_iter(match))
        return count

    def close(self, nargs: Any) -> Any:
        pass


class AsyncClientHelper:
    client: Any
    lib: Any

    def __init__(self, client: Any, lib: Any):
        self.client = client
        self.lib = lib

    async def query(
        self,
        continuation: str | None,
        limit: int | None,
        match: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
    ) -> KeyValueList:
        items: list = []
        prefix_length = len(collection) + 1 if collection else 0
        rcontinuation = continuation
        rlimit = limit
        if config and config.paging and config.page_size:
            if limit:
                if config.page_size < limit:
                    rlimit = config.page_size
            else:
                rlimit = config.page_size
        while True:
            cursor = int(rcontinuation) if rcontinuation else 0
            count = rlimit if rlimit else None
            cursor, keys = await self.client.scan(
                cursor=cursor, count=count, match=match
            )
            for key in keys:
                items.append(build_item(id=key[prefix_length:]))
            if cursor == 0:
                rcontinuation = None
                break
            if config and config.paging and not config.page_size:
                rcontinuation = str(cursor)
                break
            if rlimit and len(items) < rlimit:
                rlimit = rlimit - len(items)
            rcontinuation = str(cursor)
        return KeyValueList(items=items, continuation=rcontinuation)

    async def count(self, func: str, match: str | None, nargs: Any):
        count = 0
        if func == "dbsize":
            count = await self.client.dbsize()
        elif func == "scan_iter":
            async for _ in self.client.scan_iter(match):
                count += 1
        return count

    async def close(self, nargs: Any) -> Any:
        await self.client.aclose()


class OperationConverter:
    processor: ItemProcessor

    def __init__(self, processor: ItemProcessor):
        self.processor = processor

    def convert_exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> list:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return [db_key]

    def convert_get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> tuple[dict, str]:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        if start or end:
            args = {"key": db_key, "start": start or 0, "end": end or -1}
            func = "getrange"
        else:
            args = {"name": db_key}
            func = "get"
        return args, func

    def convert_put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        exists: bool | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        args: dict = {
            "name": db_key,
            "value": value,
        }
        if exists is False:
            args["nx"] = True
        elif exists is True:
            args["xx"] = True
        if expiry:
            args["px"] = expiry
        if returning == "old":
            args["get"] = True
        return args

    def convert_update(
        self,
        key: KeyValueKeyType,
        set: Update,
        exists: bool | None,
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
                args: dict = {"name": db_key, "amount": amount}
                if isinstance(amount, int):
                    return args, "incrby"
                else:
                    return args, "incrbyfloat"
            elif op == UpdateOp.APPEND:
                value = set.operations[0].args[0]
                args = {"key": db_key, "value": value}
                return args, "append"
        raise BadRequestError(f"Update operation not supported: {set}")

    def convert_delete(
        self,
        key: KeyValueKeyType,
        exists: bool | None,
        collection: str | None,
    ) -> list:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return [db_key]

    def convert_query(
        self,
        where: Expression | None,
        limit: int | None,
        continuation: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
    ) -> dict:
        args: dict = {}
        args["continuation"] = continuation
        args["limit"] = limit
        args["match"] = self._convert_where(where, collection)
        args["config"] = config
        args["collection"] = collection
        return args

    def convert_count(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> dict:
        if where is None and collection is None:
            return {"func": "dbsize", "match": None}
        pattern = self._convert_where(where, collection)
        return {"func": "scan_iter", "match": pattern}

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
            return {"mapping": kvs}, "mset"
        elif is_batch_op(op_parsers, StoreOperation.GET):
            keys = []
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                id = self.processor.get_id_from_key(op_parser.get_key())
                db_key = get_collection_key(col, id)
                keys.append(db_key)
            return {"keys": keys}, "mget"
        elif is_batch_op(op_parsers, StoreOperation.DELETE):
            keys = []
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                id = self.processor.get_id_from_key(op_parser.get_key())
                db_key = get_collection_key(col, id)
                keys.append(db_key)
            return keys, "delete"
        else:
            raise BadRequestError("Batch operations not supported")

    def _convert_where(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> str | None:
        prefix = get_collection_prefix(collection)
        if where is None:
            return f"{prefix}*"
        if isinstance(where, Comparison):
            if where.op == ComparisonOp.LIKE and isinstance(where.rexpr, str):
                if prefix:
                    return f"{prefix}{where.rexpr}"
                return where.rexpr
        elif isinstance(where, Function):
            if where.namespace == FunctionNamespace.BUILTIN:
                if where.name == QueryFunctionName.STARTS_WITH:
                    if prefix:
                        return f"{prefix}{where.args[1]}*"
                    return where.args[1]
                if where.name == QueryFunctionName.ENDS_WITH:
                    if prefix:
                        return f"{prefix}*{where.args[1]}"
                    return where.args[1]
                if where.name == QueryFunctionName.CONTAINS:
                    if prefix:
                        return f"{prefix}*{where.args[1]}*"
                    return where.args[1]
        raise BadRequestError(f"Condition not supported: {where!r}")


class ResultConverter:
    processor: ItemProcessor

    def __init__(self, processor: ItemProcessor):
        self.processor = processor

    def convert_exists(self, nresult: Any) -> bool:
        if nresult == 1:
            return True
        return False

    def convert_get(self, nresult: Any, key: KeyValueKeyType) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if nresult is None:
            raise NotFoundError
        return build_item(id, nresult)

    def convert_put(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
        value: KeyValueValueType,
        type: str,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if nresult is None:
            raise PreconditionFailedError
        if returning == "old":
            return build_item(id, convert_value(nresult, type))
        elif returning == "new":
            return build_item(id, convert_value(value, type))
        return build_item(id)

    def convert_update(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
        type: str,
    ) -> KeyValueItem:
        return_value = None
        if returning == "new":
            return_value = nresult
        id = self.processor.get_id_from_key(key)
        return build_item(id, convert_value(return_value, type))

    def convert_delete(self, nresult: Any):
        if nresult == 0:
            raise NotFoundError
        return None

    def convert_query(self, nresult: Any):
        return nresult

    def convert_count(self, nresult: Any):
        return nresult

    def convert_batch(
        self,
        nresult: Any,
        func: str,
        op_parsers: list[StoreOperationParser],
    ):
        result: list = []
        if func == "mget":
            for op_parser, value in zip(op_parsers, nresult):
                key = op_parser.get_key()
                id = self.processor.get_id_from_key(key)
                result.append(build_item(id=id, value=value))
        elif func == "mset":
            for op_parser in op_parsers:
                key = op_parser.get_key()
                id = self.processor.get_id_from_key(key)
                result.append(build_item(id=id))
        elif func == "delete":
            for op_parser in op_parsers:
                key = op_parser.get_key()
                id = self.processor.get_id_from_key(key)
                result.append(None)
        return result
