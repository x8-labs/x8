"""
Key Value Store on SQLite.
"""

from __future__ import annotations

__all__ = ["SQLite"]

import sqlite3
from datetime import datetime, timezone
from typing import Any

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

from .._helper import build_item, convert_value, get_collection_name
from .._models import (
    KeyValueItem,
    KeyValueKeyType,
    KeyValueList,
    KeyValueValueType,
)


class SQLite(StoreProvider):
    database: str
    table: str
    collection: str | None
    nparams: dict[str, Any]

    _client: Any
    _processor: ItemProcessor
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        database: str = ":memory:",
        table: str = "kv",
        collection: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            database:
                SQLite database. Defaults to ":memory:".
            table:
                Table name that will host the key value store.
            collection:
                Default collection.
            nparams:
                Native parameters to SQLite client.
        """
        self.database = database
        self.table = table
        self.collection = collection
        self.nparams = nparams

        self._client = None
        self._processor = ItemProcessor()
        self._op_converter = OperationConverter(self._processor, self.table)
        self._result_converter = ResultConverter(self._processor)

    def __setup__(
        self,
        context: Context | None = None,
    ) -> None:
        if self._client is not None:
            return

        self._client = sqlite3.connect(
            database=self.database,
            check_same_thread=False,
            **self.nparams,
        )
        self._create_table_if_needed()

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall, state = self._get_ncall(
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
        op_parser: StoreOperationParser,
        helper: Any,
    ) -> tuple[NCall | None, dict]:
        call = None
        state: dict = {}
        op_converter = self._op_converter
        collection: str | None = get_collection_name(self, op_parser)
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            args: dict = {
                "key": op_parser.get_key(),
                "collection": collection,
            }
            call = NCall(helper.exists, args)
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            args = {
                "key": op_parser.get_key(),
                "start": op_parser.get_start(),
                "end": op_parser.get_end(),
                "collection": collection,
            }
            call = NCall(helper.get, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, state = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
                op_parser.get_expiry(),
                op_parser.get_returning(),
                collection,
            )
            call = NCall(
                helper.execute,
                args,
                None,
                {sqlite3.IntegrityError: PreconditionFailedError},
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, state = op_converter.convert_update(
                op_parser.get_key(),
                op_parser.get_set(),
                op_parser.get_where_etag(),
                op_parser.get_returning(),
                collection,
            )
            call = NCall(helper.execute, args)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = op_converter.convert_delete(
                op_parser.get_key(),
                op_parser.get_where_etag(),
                collection,
            )
            call = NCall(helper.execute, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = op_converter.convert_query(
                op_parser.get_where(),
                op_parser.get_limit(),
                collection,
            )
            call = NCall(helper.execute, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = op_converter.convert_count(
                op_parser.get_where(),
                collection,
            )
            call = NCall(helper.execute, args)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, state = op_converter.convert_batch(
                op_parser.get_operation_parsers(),
                collection,
            )
            call = NCall(helper.batch, args)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            call = NCall(helper.close, None)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict,
        op_parser: StoreOperationParser,
    ) -> Any:
        result: Any = None
        result_converter = self._result_converter
        type = self.__component__.type
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            result = nresult
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            result = result_converter.convert_get(nresult, type)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = result_converter.convert_put(
                nresult,
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
                op_parser.get_returning(),
                state["etag"],
                type,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(
                nresult,
                op_parser.get_key(),
                op_parser.get_where_etag(),
                op_parser.get_returning(),
                state["etag"],
                type,
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = result_converter.convert_delete(
                nresult,
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
            result = result_converter.convert_batch(
                nresult,
                op_parser.get_operation_parsers(),
                state["etags"],
                type,
            )
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            pass
        return result

    def _create_table_if_needed(self, **kwargs: Any) -> None:
        cursor = self._client.cursor()
        rows = cursor.execute(
            """SELECT name FROM sqlite_master WHERE type='table'
                AND name=?""",
            (self.table,),
        ).fetchall()
        if len(rows) == 0:
            cursor.execute(
                f"""
                CREATE TABLE {self.table} (
                    collection TEXT,
                    id TEXT,
                    value BLOB,
                    etag TEXT,
                    expiry REAL,
                    PRIMARY KEY (collection, id)
                )
                """
            )
        self._client.commit()
        cursor.close()


class OperationConverter:
    processor: ItemProcessor
    table: str

    def __init__(self, processor: ItemProcessor, table: str):
        self.processor = processor
        self.table = table

    def convert_exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        query = f"""
                SELECT id, expiry FROM {self.table}
                WHERE collection = ? AND id = ?
                """
        return {
            "query": query,
            "params": (
                collection,
                id,
            ),
        }

    def convert_get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        value_column = "value"
        if start and end:
            value_column = f"substr(value, {start + 1}, {end - start + 1})"
        elif start:
            value_column = f"""substr(
                    value, {start + 1}, LENGTH(value) - {start}
                )"""
        elif end:
            value_column = f"substr(value, 1, {end + 1})"
        query = f"""
                SELECT id, {value_column}, etag, expiry
                FROM {self.table}
                WHERE collection = ? AND id = ?
                """
        return {
            "query": query,
            "params": (
                collection,
                id,
            ),
        }

    def convert_put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: bool | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
    ) -> tuple[dict, dict]:
        id = self.processor.get_id_from_key(key)
        etag = self.processor.generate_etag()
        state = {"etag": etag}
        expiry_time = (
            datetime.now(timezone.utc).timestamp() + (expiry / 1000)
            if expiry
            else 0
        )
        current_time = datetime.now(timezone.utc).timestamp()
        if where_exists is None and where_etag is None:
            query = f"""INSERT INTO {self.table} (
                            collection, id, value, etag, expiry
                        )
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (collection, id)
                        DO UPDATE SET
                        value = EXCLUDED.value,
                        etag = EXCLUDED.etag,
                        expiry = EXCLUDED.expiry
                    """
            params: tuple = (
                collection,
                id,
                value,
                etag,
                expiry_time,
            )
            return {
                "query": query,
                "params": params,
            }, state
        elif where_exists is False:
            query = f"""INSERT INTO {self.table} (
                            collection, id, value, etag, expiry
                        )
                        VALUES (?, ?, ?, ?, ?)"""
            params = (
                collection,
                id,
                value,
                etag,
                expiry_time,
            )
            return {
                "query": query,
                "params": params,
            }, state
        elif where_exists is True:
            query = f"""
                    UPDATE {self.table}
                    SET value = ?,
                    etag = ?,
                    expiry = ?
                    WHERE collection = ?
                    AND id = ?
                    AND (expiry = 0 OR expiry > ?)
                    """
            params = (
                value,
                etag,
                expiry_time,
                collection,
                id,
                current_time,
            )
            return {
                "query": query,
                "params": params,
                "rowcount": True,
            }, state
        else:
            query = f"""
                    UPDATE {self.table}
                    SET value = ?,
                    etag = ?,
                    expiry = ?
                    WHERE collection = ?
                    AND id = ?
                    AND etag = ?
                    AND (expiry = 0 OR expiry > ?)
                    """
            params = (
                value,
                etag,
                expiry_time,
                collection,
                id,
                where_etag,
                current_time,
            )
            return {
                "query": query,
                "params": params,
                "rowcount": True,
            }, state

    def convert_update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where_etag: str | None,
        returning: str | None,
        collection: str | None,
    ) -> tuple[dict, dict]:
        id = self.processor.get_id_from_key(key)
        etag = self.processor.generate_etag()
        state = {"etag": etag}
        not_supported = False
        expiry_time = 0
        if len(set.operations) == 1 and (
            set.operations[0].field == Attribute.VALUE
            or set.operations[0].field == UpdateAttribute.VALUE
        ):
            op = set.operations[0].op
            arg = set.operations[0].args[0]
            query = None
            if op == UpdateOp.INCREMENT:
                query = f"""
                        INSERT INTO {self.table}
                        (collection, id, value, etag, expiry)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(collection, id) DO UPDATE
                        SET value = value + EXCLUDED.value,
                        etag = EXCLUDED.etag
                        """
            elif op == UpdateOp.APPEND:
                query = f"""
                        INSERT INTO {self.table}
                        (collection, id, value, etag, expiry)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(collection, id) DO UPDATE
                        SET value = value || EXCLUDED.value,
                        etag = EXCLUDED.etag
                        """
            elif op == UpdateOp.PREPEND:
                query = f"""
                        INSERT INTO {self.table}
                        (collection, id, value, etag, expiry)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(collection, id) DO UPDATE
                        SET value = EXCLUDED.value || value,
                        etag = EXCLUDED.etag
                        """
            else:
                not_supported = True
        else:
            not_supported = True
        if not_supported:
            raise BadRequestError(f"Update operation not supported {set}")
        params: tuple = (
            collection,
            id,
            arg,
            etag,
            expiry_time,
        )
        if where_etag:
            query = f"{query} WHERE {self.table}.etag = ?"
            params = params + (where_etag,)
        if returning == "new":
            query = f"{query} RETURNING value"
        else:
            query = f"{query} RETURNING id"
        return {
            "query": query,
            "params": params,
        }, state

    def convert_delete(
        self,
        key: KeyValueKeyType,
        where_etag: str | None,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        query = f"""
                    DELETE FROM {self.table}
                    WHERE collection = ? AND id = ?
                    """
        if where_etag:
            query = f"{query} AND etag = ?"
            params: tuple = (
                collection,
                id,
                where_etag,
            )
        else:
            params = (
                collection,
                id,
            )
        query = f"{query} RETURNING expiry"
        return {
            "query": query,
            "params": params,
        }

    def convert_query(
        self,
        where: Expression | None,
        limit: int | None,
        collection: str | None,
    ) -> dict:
        where_str = self._convert_where(where, collection)
        query = f"SELECT id FROM {self.table} WHERE {where_str}"
        if limit:
            query = f"{query} LIMIT {limit}"
        return {"query": query, "fetchall": True}

    def convert_count(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> dict:
        where_str = self._convert_where(where, collection)
        query = f"SELECT COUNT(*) FROM {self.table} WHERE {where_str}"
        return {"query": query}

    def convert_batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
    ) -> tuple[dict, dict]:
        ops: list = []
        states: list = []
        for op_parser in op_parsers:
            collection = op_parser.get_collection_name() or collection
            id = self.processor.get_id_from_key(op_parser.get_key())
            if op_parser.op_equals(StoreOperation.PUT):
                etag = self.processor.generate_etag()
                query = f"""INSERT INTO {self.table} (
                            collection, id, value, etag, expiry
                        )
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (collection, id)
                        DO UPDATE SET
                        value = EXCLUDED.value,
                        etag = EXCLUDED.etag,
                        expiry = EXCLUDED.expiry
                        """
                params: tuple = (
                    collection,
                    id,
                    op_parser.get_value(),
                    etag,
                    0,
                )
                ops.append({"query": query, "params": params})
                states.append({"etag": etag})
            elif op_parser.op_equals(StoreOperation.GET):
                query = f"""
                        SELECT id, value, etag, expiry
                        FROM {self.table}
                        WHERE collection = ? AND id = ?
                        """
                params = (
                    collection,
                    id,
                )
                ops.append({"query": query, "params": params})
                states.append(None)
            elif op_parser.op_equals(StoreOperation.DELETE):
                query = f"""
                        DELETE FROM {self.table}
                        WHERE collection = ? AND id = ?
                        """
                params = (
                    collection,
                    id,
                )
                ops.append({"query": query, "params": params})
                states.append(None)
            else:
                raise BadRequestError(
                    f"Operation not supported in batch: {op_parser}"
                )
        return {"ops": ops}, {"etags": states}

    def convert_evict(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> dict:
        id = self.processor.get_id_from_key(key)
        current_time = datetime.now(timezone.utc).timestamp()
        query = f"""
                DELETE FROM {self.table}
                WHERE collection = ? AND id = ?
                AND expiry != 0
                AND expiry < ?
                """
        return {
            "query": query,
            "params": (
                collection,
                id,
                current_time,
            ),
        }

    def _convert_where(
        self, where: Expression | None, collection: str | None
    ) -> str:
        if collection:
            query = f"collection = '{collection}'"
        else:
            query = "collection = null"
        if where is None:
            return query
        if isinstance(where, Comparison):
            if where.op == ComparisonOp.LIKE and isinstance(where.rexpr, str):
                return f"{query} AND id LIKE {where.rexpr}"
        elif isinstance(where, Function):
            if where.namespace == FunctionNamespace.BUILTIN:
                if where.name == QueryFunctionName.STARTS_WITH:
                    return f"{query} AND id LIKE '{where.args[1]}%'"
                if where.name == QueryFunctionName.ENDS_WITH:
                    return f"{query} AND id LIKE '%{where.args[1]}'"
                if where.name == QueryFunctionName.CONTAINS:
                    return f"{query} AND id LIKE '%{where.args[1]}%'"
        raise BadRequestError(f"Condition not supported: {where!r}")


class ResultConverter:
    processor: ItemProcessor

    def __init__(
        self,
        processor: ItemProcessor,
    ):
        self.processor = processor

    def convert_get(self, nresult: Any, type: str) -> KeyValueItem:
        (id, value, etag, expiry) = nresult
        return build_item(id=id, value=convert_value(value, type), etag=etag)

    def convert_put(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where_exists: bool | None,
        where_etag: str | None,
        returning: str | None,
        etag: str,
        type: str,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        if where_exists is True or where_etag:
            if nresult == 0:
                raise PreconditionFailedError
        return_value = None
        if returning == "new":
            return_value = value
        return build_item(
            id=id, value=convert_value(return_value, type), etag=etag
        )

    def convert_update(
        self,
        nresult: Any,
        key: KeyValueKeyType,
        where_etag: str | None,
        returning: str | None,
        etag: str,
        type: str,
    ) -> KeyValueItem:
        id = self.processor.get_id_from_key(key)
        return_value = None
        if nresult is None:
            raise PreconditionFailedError
        if returning == "new":
            (return_value,) = nresult
        return build_item(
            id=id, value=convert_value(return_value, type), etag=etag
        )

    def convert_delete(self, nresult: Any, where_etag: str | None):
        if nresult is None:
            if where_etag:
                raise PreconditionFailedError
            raise NotFoundError
        current_time = datetime.now(timezone.utc).timestamp()
        if nresult[0] != 0 and current_time > nresult[0]:
            raise NotFoundError
        return None

    def convert_query(self, nresult: Any) -> KeyValueList:
        items: list = []
        for raw_item in nresult:
            items.append(build_item(id=raw_item[0]))
        return KeyValueList(items=items)

    def convert_count(self, nresult: Any) -> int:
        return nresult[0]

    def convert_batch(
        self,
        nresult: Any,
        op_parsers: list[StoreOperationParser],
        states: list[dict],
        type: str,
    ) -> list[Any]:
        result: list = []
        for op_parser, op_result, state in zip(op_parsers, nresult, states):
            id = self.processor.get_id_from_key(op_parser.get_key())
            if op_parser.op_equals(StoreOperation.PUT):
                result.append(build_item(id=id, etag=state["etag"]))
            elif op_parser.op_equals(StoreOperation.GET):
                expiry = op_result[3]
                current_time = datetime.now(timezone.utc).timestamp()
                has_expired = expiry != 0 and current_time > expiry
                if has_expired:
                    result.append(None)
                else:
                    result.append(
                        build_item(
                            id=id,
                            value=convert_value(op_result[1], type),
                            etag=op_result[2],
                        )
                    )
            elif op_parser.op_equals(StoreOperation.DELETE):
                result.append(None)
        return result


class ClientHelper:
    client: Any
    op_converter: OperationConverter

    def __init__(self, client: Any, op_converter: OperationConverter):
        self.client = client
        self.op_converter = op_converter

    def exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> bool:
        args = self.op_converter.convert_exists(key, collection)
        cursor = self.client.cursor()
        try:
            cursor.execute(args["query"], args["params"])
            nresult = cursor.fetchone()
            if nresult is None:
                return False
            (id, expiry) = nresult
            current_time = datetime.now(timezone.utc).timestamp()
            has_expired = expiry != 0 and current_time > expiry
            if has_expired:
                args = self.op_converter.convert_evict(key, collection)
                cursor.execute(args["query"], args["params"])
                return False
        finally:
            self.client.commit()
            cursor.close()
        return True

    def get(
        self,
        key: KeyValueKeyType,
        collection: str | None,
        start: int | None,
        end: int | None,
    ) -> Any:
        args = self.op_converter.convert_get(key, start, end, collection)
        cursor = self.client.cursor()
        try:
            cursor.execute(args["query"], args["params"])
            nresult = cursor.fetchone()
            if nresult is None:
                raise NotFoundError
            (
                id,
                value,
                etag,
                expiry,
            ) = nresult
            current_time = datetime.now(timezone.utc).timestamp()
            has_expired = expiry != 0 and current_time > expiry
            if has_expired:
                args = self.op_converter.convert_evict(key, collection)
                cursor.execute(args["query"], args["params"])
                raise NotFoundError
        finally:
            self.client.commit()
            cursor.close()
        return nresult

    def execute(
        self,
        query: str,
        params: tuple | None = None,
        rowcount: bool = False,
        fetchall: bool = False,
    ) -> Any:
        cursor = self.client.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if rowcount:
                return cursor.rowcount
            if fetchall:
                return cursor.fetchall()
            return cursor.fetchone()
        finally:
            self.client.commit()
            cursor.close()

    def batch(self, ops: list) -> list[Any]:
        nresult = []
        cursor = self.client.cursor()
        try:
            for op in ops:
                if "params" in op:
                    cursor.execute(op["query"], op["params"])
                else:
                    cursor.execute(op["query"])
                nresult.append(cursor.fetchone())
        finally:
            self.client.commit()
            cursor.close()
        return nresult

    def close(self) -> Any:
        self.client.close()
