"""
Secret Store on top of SQLite.
"""

__all__ = ["SQLite"]

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Any

from x8.core import Context, Operation, Response
from x8.core.exceptions import NotFoundError, PreconditionFailedError
from x8.ql import QueryProcessor
from x8.storage._common import ItemProcessor, StoreOperation, StoreProvider

from .._models import (
    SecretItem,
    SecretKey,
    SecretList,
    SecretProperties,
    SecretVersion,
)


class SQLite(StoreProvider):
    database: str
    nparams: dict[str, Any]

    _client: Any
    _processor: ItemProcessor

    def __init__(
        self,
        database: str = ":memory:",
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            database:
                SQLite database. Defaults to ":memory:".
            nparams:
                Native parameters to SQLite client.
        """
        self.database = database
        self.nparams = nparams

        self._client = None
        self._processor = ItemProcessor()

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
        cursor = self._client.cursor()
        rows = cursor.execute(
            """SELECT name FROM sqlite_master WHERE type='table'
                AND (name=? or name=?)""",
            ("items", "versions"),
        ).fetchall()
        if len(rows) == 0:
            self._create_tables(cursor)
        cursor.close()

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        client = self._client
        op_parser = self.get_op_parser(operation)
        processor = self._processor
        nresult: Any = None
        result: Any = None
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            version = op_parser.get_version()
            query_version = version
            cursor = client.cursor()
            try:
                if query_version is None:
                    nresult = cursor.execute(
                        """SELECT version FROM items
                            WHERE id = ? AND deleted = 0""",
                        (id,),
                    ).fetchone()
                    if nresult is None:
                        raise NotFoundError
                    (query_version,) = nresult
                nresult = cursor.execute(
                    """SELECT value FROM versions WHERE id = ?
                        AND version = ?""",
                    (id, query_version),
                ).fetchone()
                (value,) = nresult
                result = SecretItem(
                    key=SecretKey(id=id, version=query_version),
                    value=value,
                )
            finally:
                cursor.close()
        # GET metadata
        elif op_parser.op_equals(StoreOperation.GET_METADATA):
            id = op_parser.get_id_as_str()
            cursor = client.cursor()
            try:
                nresult = cursor.execute(
                    """SELECT metadata, created_time FROM items
                        WHERE id = ? AND deleted = 0""",
                    (id,),
                ).fetchone()
                if nresult is None:
                    raise NotFoundError
                (metadata, created_time) = nresult
                result = SecretItem(
                    key=SecretKey(id=id),
                    metadata=json.loads(metadata),
                    properties=SecretProperties(
                        created_time=Helper.convert_to_timestamp(created_time)
                    ),
                )
            finally:
                cursor.close()
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            cursor = client.cursor()
            try:
                versions: list = []
                nresult = cursor.execute(
                    """SELECT id FROM items
                        WHERE id = ? AND deleted = 0""",
                    (id,),
                ).fetchone()
                if nresult is None:
                    raise NotFoundError
                nresult = cursor.execute(
                    """SELECT version, created_time FROM versions
                        WHERE id = ? ORDER BY created_time DESC""",
                    (id,),
                ).fetchall()
                if len(nresult) == 0:
                    raise NotFoundError
                for item in nresult:
                    (version, created_time) = item
                    versions.append(
                        SecretVersion(
                            version=str(version),
                            created_time=Helper.convert_to_timestamp(
                                created_time
                            ),
                        )
                    )
                result = SecretItem(key=SecretKey(id=id), versions=versions)
            finally:
                cursor.close()
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            metadata = op_parser.get_metadata()
            exists = op_parser.get_where_exists()
            version = str(uuid.uuid4())
            created_time = datetime.now()
            cursor = client.cursor()
            insert = True
            if exists is None or exists is False:
                try:
                    nresult = cursor.execute(
                        """INSERT INTO items (id, version, metadata,
                            created_time, deleted) VALUES (?, ?, ?, ?, ?)""",
                        (
                            id,
                            version,
                            json.dumps(
                                metadata if metadata is not None else {}
                            ),
                            created_time,
                            0,
                        ),
                    )
                except sqlite3.IntegrityError:
                    insert = False
                    if exists is False:
                        raise PreconditionFailedError
            if exists is False or exists is True or insert is False:
                nresult = cursor.execute(
                    """UPDATE items SET version = ?,
                        metadata = ? WHERE id = ? AND deleted = 0
                        RETURNING id""",
                    (
                        version,
                        json.dumps(metadata if metadata is not None else {}),
                        id,
                    ),
                ).fetchone()
                if nresult is None and exists is True:
                    raise PreconditionFailedError
            nresult = cursor.execute(
                """INSERT INTO versions (id, version, value,
                    created_time) VALUES (?, ?, ?, ?)""",
                (id, version, value, created_time),
            )
            cursor.close()
            result = SecretItem(key=SecretKey(id=id, version=version))
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            version = str(uuid.uuid4())
            created_time = datetime.now()
            cursor = client.cursor()
            try:
                nresult = cursor.execute(
                    """UPDATE items SET version = ?
                        WHERE id = ? AND deleted = 0 RETURNING *""",
                    (version, id),
                ).fetchone()
                if nresult is None:
                    raise NotFoundError
                nresult = cursor.execute(
                    """INSERT INTO versions (id, version, value,
                        created_time) VALUES (?, ?, ?, ?)""",
                    (id, version, value, created_time),
                )
                result = SecretItem(key=SecretKey(id=id, version=version))
            finally:
                cursor.close()
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            id = op_parser.get_id_as_str()
            metadata = op_parser.get_metadata()
            cursor = client.cursor()
            try:
                nresult = cursor.execute(
                    """UPDATE items SET metadata = ?
                        WHERE id = ? AND deleted = 0 RETURNING id""",
                    (json.dumps(metadata if metadata is not None else {}), id),
                ).fetchone()
                if nresult is None:
                    raise NotFoundError
                result = SecretItem(
                    key=SecretKey(id=id),
                    metadata=metadata if metadata is not None else {},
                )
            finally:
                cursor.close()
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            cursor = client.cursor()
            try:
                nresult = cursor.execute(
                    "DELETE FROM items WHERE id = ? RETURNING id",
                    (id,),
                ).fetchone()
                if nresult is None:
                    raise NotFoundError
                nresult = cursor.execute(
                    "DELETE FROM versions WHERE id = ?", (id,)
                )
                result = None
            finally:
                cursor.close()
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            cursor = client.cursor()
            items = []
            try:
                nresult = cursor.execute(
                    """SELECT id, metadata, created_time FROM items
                        WHERE deleted=0 ORDER BY id"""
                )
                for item in nresult:
                    (id, metadata, created_time) = item
                    items.append(
                        SecretItem(
                            key=SecretKey(id=id),
                            metadata=json.loads(metadata),
                            properties=SecretProperties(
                                created_time=Helper.convert_to_timestamp(
                                    created_time
                                )
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
            finally:
                cursor.close()
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            cursor = client.cursor()
            items = []
            try:
                nresult = cursor.execute(
                    """SELECT id, metadata, created_time FROM items
                        WHERE deleted=0 ORDER BY id"""
                )
                for item in nresult:
                    (id, metadata, created_time) = item
                    items.append(
                        SecretItem(
                            key=SecretKey(id=id),
                            metadata=json.loads(metadata),
                            properties=SecretProperties(
                                created_time=Helper.convert_to_timestamp(
                                    created_time
                                )
                            ),
                        )
                    )
                result = QueryProcessor.count_items(
                    items=items,
                    where=op_parser.get_where(),
                    field_resolver=processor.resolve_root_field,
                )
            finally:
                cursor.close()
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            pass
        else:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        return Response(result=result, native=dict(result=nresult))

    def _create_tables(self, cursor: Any, **kwargs: Any) -> None:
        cursor.execute(
            """CREATE TABLE items (id TEXT PRIMARY KEY,
                version TEXT, metadata JSON, created_time TIMESTAMP,
                deleted BOOLEAN, deleted_time TIMESTAMP, UNIQUE(id))"""
        )
        cursor.execute(
            """CREATE TABLE versions (id TEXT, version TEXT,
                value TEXT, created_time TIMESTAMP,
                PRIMARY KEY (id, version),
                UNIQUE(id, version))"""
        )


class Helper:
    @staticmethod
    def convert_to_timestamp(timestamp: str):
        return datetime.fromisoformat(timestamp).astimezone().timestamp()
