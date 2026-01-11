"""
Key Value Store on Redis.
"""

from __future__ import annotations

__all__ = ["Redis"]

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


class Redis(RedisProvider, StoreProvider):
    collection: str | None
    nparams: dict[str, Any]

    _lib: Any
    _alib: Any

    _client: Any
    _aclient: Any
    _processor: ItemProcessor
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
        self._processor = ItemProcessor()
        self._op_converter = OperationConverter(processor=self._processor)
        self._result_converter = ResultConverter(processor=self._processor)
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
            ClientHelper(
                self._client,
                self._lib,
                self._op_converter,
            ),
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
            AsyncClientHelper(
                self._aclient,
                self._alib,
                self._op_converter,
            ),
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
    ) -> tuple[NCall | None, dict]:
        call = None
        state: dict = {}
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
            args = op_converter.convert_get(
                op_parser.get_key(),
                op_parser.get_start(),
                op_parser.get_end(),
                collection,
            )
            call = NCall(client.hmget, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            etag = self._processor.generate_etag()
            args = {
                "key": op_parser.get_key(),
                "value": op_parser.get_value(),
                "where_exists": op_parser.get_where_exists(),
                "where_etag": op_parser.get_where_etag(),
                "expiry": op_parser.get_expiry(),
                "returning": op_parser.get_returning(),
                "collection": collection,
                "etag": etag,
                "nargs": nargs,
            }
            state["etag"] = etag
            call = NCall(
                client_helper.put,
                args,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            etag = self._processor.generate_etag()
            args = {
                "key": op_parser.get_key(),
                "set": op_parser.get_set(),
                "where_exists": op_parser.get_where_exists(),
                "where_etag": op_parser.get_where_etag(),
                "returning": op_parser.get_returning(),
                "collection": collection,
                "etag": etag,
                "nargs": nargs,
            }
            state["etag"] = etag
            call = NCall(client_helper.update, args)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = {
                "key": op_parser.get_key(),
                "where_exists": op_parser.get_where_exists(),
                "where_etag": op_parser.get_where_etag(),
                "collection": collection,
                "nargs": nargs,
            }
            call = NCall(client_helper.delete, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = op_converter.convert_query(
                op_parser.get_where(),
                op_parser.get_limit(),
                op_parser.get_continuation(),
                get_query_config(op_parser),
                collection,
            )
            args["nargs"] = nargs
            call = NCall(client_helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = op_converter.convert_count(
                op_parser.get_where(),
                collection,
            )
            args["nargs"] = nargs
            call = NCall(client_helper.count, args)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args = {
                "op_parsers": op_parser.get_operation_parsers(),
                "collection": collection,
                "nargs": nargs,
            }
            call = NCall(client_helper.batch, args)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(client_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict,
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
                state["etag"],
                type,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(
                nresult,
                op_parser.get_key(),
                op_parser.get_returning(),
                state["etag"],
                type,
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = result_converter.convert_delete(
                nresult,
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            result = result_converter.convert_query(nresult)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = result_converter.convert_count(nresult)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = nresult
        return result


class OperationConverter:
    processor: ItemProcessor

    def __init__(self, processor: ItemProcessor):
        self.processor = processor

    def convert_to_db_key(
        self, key: KeyValueKeyType, collection: str | None
    ) -> str:
        id = self.processor.get_id_from_key(key)
        db_key = get_collection_key(collection, id)
        return db_key

    def convert_exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> list:
        db_key = self.convert_to_db_key(key, collection)
        return [db_key]

    def convert_get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> dict:
        db_key = self.convert_to_db_key(key, collection)
        args = {"name": db_key, "keys": ["value", "etag"]}
        return args

    def convert_put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: str | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
        etag: str,
    ) -> dict:
        db_key = self.convert_to_db_key(key, collection)
        args: dict = {
            "name": db_key,
            "mapping": {
                "value": value,
                "etag": etag,
            },
        }
        return args

    def convert_update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
        etag: str,
    ) -> tuple[dict, str]:
        db_key = self.convert_to_db_key(key, collection)
        if len(set.operations) == 1 and (
            set.operations[0].field == Attribute.VALUE
            or set.operations[0].field == UpdateAttribute.VALUE
        ):
            op = set.operations[0].op
            if op == UpdateOp.INCREMENT:
                amount = set.operations[0].args[0]
                args: dict = {
                    "name": db_key,
                    "key": "value",
                    "amount": amount,
                }
                if isinstance(amount, int):
                    return args, "hincrby"
                else:
                    return args, "hincrbyfloat"
        raise BadRequestError(f"Update operation not supported: {set}")

    def convert_delete(
        self,
        key: KeyValueKeyType,
        where_exists: bool | None,
        where_etag: str | None,
        collection: str | None,
    ) -> list:
        db_key = self.convert_to_db_key(key, collection)
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
    ) -> tuple[list, str]:
        def is_batch_op(
            op_parsers: list[StoreOperationParser], op: str
        ) -> bool:
            for op_parser in op_parsers:
                if not op_parser.op_equals(op):
                    return False
            return True

        args_list: list = []
        if is_batch_op(op_parsers, StoreOperation.PUT):
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                db_key = self.convert_to_db_key(op_parser.get_key(), col)
                value = op_parser.get_value()
                etag = self.processor.generate_etag()
                args_list.append(
                    {
                        "name": db_key,
                        "mapping": {
                            "value": value,
                            "etag": etag,
                        },
                    }
                )
            return args_list, "hset"
        elif is_batch_op(op_parsers, StoreOperation.GET):
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                db_key = self.convert_to_db_key(op_parser.get_key(), col)
                args_list.append({"name": db_key, "keys": ["value", "etag"]})
            return args_list, "hmget"
        elif is_batch_op(op_parsers, StoreOperation.DELETE):
            for op_parser in op_parsers:
                col = op_parser.get_collection_name() or collection
                db_key = self.convert_to_db_key(op_parser.get_key(), col)
                args_list.append(db_key)
            return args_list, "delete"
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

    def convert_get(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if not nresult or not nresult[1]:
            raise NotFoundError
        value = nresult[0]
        if start and end:
            eend = end + 1
            value = value[start:eend]
        elif start:
            value = value[start:]
        elif end:
            eend = end + 1
            value = value[:eend]
        return build_item(id=id, value=value, etag=nresult[1])

    def convert_put(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
        value: KeyValueValueType,
        etag: str,
        type: str,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if not nresult:
            raise PreconditionFailedError
        elif returning == "new":
            return build_item(id, convert_value(value, type), etag=etag)
        return build_item(id, etag=etag)

    def convert_update(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        returning: str | None,
        etag: str,
        type: str,
    ) -> KeyValueItem:
        if nresult is None:
            raise PreconditionFailedError
        return_value = None
        if returning == "new":
            return_value = nresult
        id = self.processor.get_id_from_key(key)
        return build_item(id, convert_value(return_value, type), etag=etag)

    def convert_delete(
        self,
        nresult: Any,
        where_exists: bool | None,
        where_etag: str | None,
    ):
        if nresult == 0:
            if where_exists is not None or where_etag:
                raise PreconditionFailedError
            raise NotFoundError
        return None

    def convert_query(self, nresult: Any):
        return nresult

    def convert_count(self, nresult: Any):
        return nresult


class ClientHelper:
    client: Any
    lib: Any
    op_converter: OperationConverter

    def __init__(
        self,
        client: Any,
        lib: Any,
        op_converter: OperationConverter,
    ):
        self.client = client
        self.lib = lib
        self.op_converter = op_converter

    def put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: str | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
        etag: str,
        nargs: Any,
    ) -> Any:
        args = self.op_converter.convert_put(
            key,
            value,
            where_exists,
            where_etag,
            expiry,
            returning,
            collection,
            etag,
        )
        if expiry is None and where_exists is None and where_etag is None:
            NCall(self.client.hset, args, nargs).invoke()
            return True
        db_key = self.op_converter.convert_to_db_key(key, collection)
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(db_key)
                    if where_exists is True:
                        if not pipe.exists(db_key):
                            pipe.unwatch()
                            return False
                    elif where_exists is False:
                        if pipe.exists(db_key):
                            return False
                    elif where_etag:
                        etag = pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            return False
                    pipe.multi()
                    pipe.hset(**args)
                    if expiry:
                        pipe.pexpire(db_key, expiry)
                    pipe.execute()
                    return True
                except self.lib.WatchError:
                    continue

    def update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
        etag: str,
        nargs: Any,
    ) -> Any:
        args, func = self.op_converter.convert_update(
            key,
            set,
            where_exists,
            where_etag,
            returning,
            collection,
            etag,
        )
        db_key = self.op_converter.convert_to_db_key(key, collection)
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(db_key)
                    if where_etag:
                        etag = pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            return None
                    pipe.multi()
                    if func == "hincrby":
                        pipe.hincrby(**args)
                    elif func == "hincrbyfloat":
                        pipe.hincrbyfloat(**args)
                    pipe.hset(db_key, "etag", etag)
                    responses = pipe.execute()
                    return responses[0]
                except self.lib.WatchError:
                    continue

    def delete(
        self,
        key: KeyValueKeyType,
        where_exists: bool | None,
        where_etag: str | None,
        collection: str | None,
        nargs: Any,
    ) -> Any:
        args = self.op_converter.convert_delete(
            key, where_exists, where_etag, collection
        )
        if where_exists is None and where_etag is None:
            return NCall(self.client.delete, args).invoke()
        db_key = self.op_converter.convert_to_db_key(key, collection)
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(db_key)
                    if where_etag:
                        etag = pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            return False
                        pipe.delete(db_key)
                        return 1
                except self.lib.WatchError:
                    continue

    def query(
        self,
        continuation: str | None,
        limit: int | None,
        match: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
        nargs: Any,
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

    def batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
        nargs: Any,
    ):
        args_list, func = self.op_converter.convert_batch(
            op_parsers, collection
        )
        result: list = []
        if func == "hset":
            with self.client.pipeline() as pipe:
                pipe.multi()
                for arg, op_parser in zip(args_list, op_parsers):
                    pipe.hset(**arg)
                    result.append(
                        build_item(
                            id=self.op_converter.processor.get_id_from_key(
                                op_parser.get_key()
                            ),
                            etag=arg["mapping"]["etag"],
                        )
                    )
                pipe.execute()
        elif func == "hmget":
            with self.client.pipeline() as pipe:
                pipe.multi()
                for arg in args_list:
                    pipe.hmget(**arg)
                responses = pipe.execute()
            for op_parser, response in zip(op_parsers, responses):
                response.append(
                    build_item(
                        id=self.op_converter.processor.get_id_from_key(
                            op_parser.get_key()
                        ),
                        value=response[0],
                        etag=response[1],
                    )
                )
        elif func == "delete":
            NCall(self.client.delete, args_list).invoke()
            result = [None] * len(args_list)
        return result

    def close(self, nargs: Any) -> Any:
        pass


class AsyncClientHelper:
    client: Any
    lib: Any
    op_converter: OperationConverter

    def __init__(
        self,
        client: Any,
        lib: Any,
        op_converter: OperationConverter,
    ):
        self.client = client
        self.lib = lib
        self.op_converter = op_converter

    async def put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: str | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
        etag: str,
        nargs: Any,
    ) -> Any:
        args = self.op_converter.convert_put(
            key,
            value,
            where_exists,
            where_etag,
            expiry,
            returning,
            collection,
            etag,
        )
        db_key = self.op_converter.convert_to_db_key(key, collection)
        if expiry is None and where_exists is None and where_etag is None:
            await NCall(self.client.hset, args, nargs).ainvoke()
            return True

        async with self.client.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(db_key)
                    if where_exists is True:
                        if not await pipe.exists(db_key):
                            await pipe.unwatch()
                            return False
                    elif where_exists is False:
                        if await pipe.exists(db_key):
                            await pipe.unwatch()
                            return False
                    elif where_etag:
                        etag = await pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            await pipe.unwatch()
                            return False
                    pipe.multi()
                    await pipe.hset(**args)
                    if expiry:
                        await pipe.pexpire(db_key, expiry)
                    await pipe.execute()
                    return True
                except self.lib.WatchError:
                    continue

    async def update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
        etag: str,
        nargs: Any,
    ) -> Any:
        args, func = self.op_converter.convert_update(
            key,
            set,
            where_exists,
            where_etag,
            returning,
            collection,
            etag,
        )
        db_key = self.op_converter.convert_to_db_key(key, collection)
        async with self.client.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(db_key)
                    if where_etag:
                        etag = await pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            return None
                    pipe.multi()
                    if func == "hincrby":
                        await pipe.hincrby(**args)
                    elif func == "hincrbyfloat":
                        await pipe.hincrbyfloat(**args)
                    await pipe.hset(db_key, "etag", etag)
                    responses = await pipe.execute()
                    return responses[0]
                except self.lib.WatchError:
                    continue

    async def delete(
        self,
        key: KeyValueKeyType,
        where_exists: bool | None,
        where_etag: str | None,
        collection: str | None,
        nargs: Any,
    ) -> Any:
        args = self.op_converter.convert_delete(
            key, where_exists, where_etag, collection
        )
        if where_exists is None and where_etag is None:
            return await NCall(self.client.delete, args).ainvoke()
        db_key = self.op_converter.convert_to_db_key(key, collection)
        async with self.client.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(db_key)
                    if where_etag:
                        etag = await pipe.hget(db_key, "etag")
                        if isinstance(etag, bytes):
                            etag = etag.decode()
                        if not etag or etag != where_etag:
                            return False
                        await pipe.delete(db_key)
                        return 1
                except self.lib.WatchError:
                    continue

    async def query(
        self,
        continuation: str | None,
        limit: int | None,
        match: str | None,
        config: KeyValueQueryConfig | None,
        collection: str | None,
        nargs: Any,
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

    async def count(
        self,
        func: str,
        match: str | None,
        nargs: Any,
    ):
        count = 0
        if func == "dbsize":
            count = await self.client.dbsize()
        elif func == "scan_iter":
            async for _ in self.client.scan_iter(match):
                count += 1
        return count

    async def batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
        nargs: Any,
    ):
        args_list, func = self.op_converter.convert_batch(
            op_parsers, collection
        )
        result: list = []
        if func == "hset":
            async with self.client.pipeline() as pipe:
                for arg, op_parser in zip(args_list, op_parsers):
                    await pipe.hset(**arg)
                    result.append(
                        build_item(
                            id=self.op_converter.processor.get_id_from_key(
                                op_parser.get_key()
                            ),
                            etag=arg["mapping"]["etag"],
                        )
                    )
                await pipe.execute()
        elif func == "hmget":
            async with self.client.pipeline() as pipe:
                for arg in args_list:
                    await pipe.hmget(**arg)
                responses = await pipe.execute()
            for op_parser, response in zip(op_parsers, responses):
                result.append(
                    build_item(
                        id=self.op_converter.processor.get_id_from_key(
                            op_parser.get_key()
                        ),
                        value=response[0] if response else None,
                        etag=response[1] if len(response) > 1 else None,
                    )
                )

        elif func == "delete":
            await NCall(self.client.delete, args_list).ainvoke()
            result = [None] * len(args_list)

        return result

    async def close(self, nargs: Any) -> Any:
        await self.client.aclose()
