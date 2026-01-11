"""
In Memory Key Value Store.
"""

from __future__ import annotations

__all__ = ["Memory"]

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from x8.core import Context, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import QueryProcessor, UpdateOp
from x8.storage._common import (
    Attribute,
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    UpdateAttribute,
)

from .._helper import build_item, convert_value, get_collection_name
from .._models import KeyValueList


class Memory(StoreProvider):
    collection: str | None

    # (collection, key, item)
    _db: dict[str | None, dict[str, dict]]
    _processor: ItemProcessor
    _lock: Lock

    def __init__(
        self,
        collection: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            collection:
                Collection name.
        """
        self.collection = collection

        self._processor = ItemProcessor()
        self._db = dict()
        self._lock = Lock()

    def __setup__(self, context: Context | None = None) -> None:
        pass

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        def check_etag_lazy_evict(
            collection, db_key, etag, locked=False
        ) -> bool:
            if not (collection in self._db and db_key in self._db[collection]):
                return False
            if has_expired(self._db[collection][db_key]):
                if not locked:
                    with self._lock:
                        if has_expired(self._db[collection][db_key]):
                            self._db[collection].pop(db_key)
                            if len(self._db[collection]) == 0:
                                self._db.pop(collection)
                else:
                    self._db[collection].pop(db_key)
                    if len(self._db[collection]) == 0:
                        self._db.pop(collection)
                return False
            return self._db[collection][db_key]["etag"] == etag

        def check_exists_lazy_evict(collection, db_key, locked=False) -> bool:
            if not (collection in self._db and db_key in self._db[collection]):
                return False
            if has_expired(self._db[collection][db_key]):
                if not locked:
                    with self._lock:
                        if has_expired(self._db[collection][db_key]):
                            self._db[collection].pop(db_key)
                            if len(self._db[collection]) == 0:
                                self._db.pop(collection)
                else:
                    self._db[collection].pop(db_key)
                    if len(self._db[collection]) == 0:
                        self._db.pop(collection)
                return False
            return True

        def has_expired(item) -> bool:
            expiry_time: float | None = item["expiry"]
            current_time = datetime.now(timezone.utc).timestamp()
            if expiry_time and current_time > expiry_time:
                return True
            return False

        def get_value(item) -> Any:
            return item["value"]

        def set_value(item, value) -> Any:
            item["value"] = value

        def get_db_key(
            op_parser: StoreOperationParser, collection: str | None
        ):
            key = op_parser.get_key()
            id = processor.get_id_from_key(key)
            return id

        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        processor = self._processor
        result: Any = None
        collection: str | None = get_collection_name(self, op_parser)

        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            db_key = get_db_key(op_parser, collection)
            result = check_exists_lazy_evict(collection, db_key, False)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            db_key = get_db_key(op_parser, collection)
            start = op_parser.get_start()
            end = op_parser.get_end()
            if check_exists_lazy_evict(collection, db_key, False):
                value = self._db[collection][db_key]["value"]
                etag = self._db[collection][db_key]["etag"]
            else:
                raise NotFoundError
            value = convert_value(value, self.__component__.type)
            if start and end:
                eend = end + 1
                value = value[start:eend]
            elif start:
                value = value[start:]
            elif end:
                eend = end + 1
                value = value[:eend]
            result = build_item(id=db_key, value=value, etag=etag)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            db_key = get_db_key(op_parser, collection)
            value = op_parser.get_value()
            exists = op_parser.get_where_exists()
            where_etag = op_parser.get_where_etag()
            expiry = op_parser.get_expiry()
            returning = op_parser.get_returning()
            etag = self._processor.generate_etag()
            if expiry:
                expiry_time = datetime.now(timezone.utc).timestamp() + (
                    expiry / 1000
                )
            else:
                expiry_time = None
            with self._lock:
                return_value: Any = None
                if returning == "old":
                    if check_exists_lazy_evict(collection, db_key, True):
                        return_value = get_value(self._db[collection][db_key])
                elif returning == "new":
                    return_value = value
                if exists is False:
                    if check_exists_lazy_evict(collection, db_key, True):
                        raise PreconditionFailedError
                elif exists is True:
                    if not check_exists_lazy_evict(collection, db_key, True):
                        raise PreconditionFailedError
                if where_etag is not None:
                    if not check_etag_lazy_evict(
                        collection, db_key, where_etag, True
                    ):
                        raise PreconditionFailedError
                if collection not in self._db:
                    self._db[collection] = dict()
                self._db[collection][db_key] = {
                    "value": value,
                    "expiry": expiry_time,
                    "etag": etag,
                }
            result = build_item(
                id=db_key,
                value=convert_value(return_value, self.__component__.type),
                etag=etag,
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            db_key = get_db_key(op_parser, collection)
            returning = op_parser.get_returning()
            set = op_parser.get_set()
            where_etag = op_parser.get_where_etag()
            etag = self._processor.generate_etag()
            return_value = None
            not_supported = False
            with self._lock:
                if len(set.operations) == 1 and (
                    set.operations[0].field == Attribute.VALUE
                    or set.operations[0].field == UpdateAttribute.VALUE
                ):
                    op = set.operations[0].op
                    arg = set.operations[0].args[0]
                    if where_etag is not None:
                        if not check_etag_lazy_evict(
                            collection, db_key, where_etag, True
                        ):
                            raise PreconditionFailedError
                    if check_exists_lazy_evict(collection, db_key, True):
                        if returning == "old":
                            return_value = get_value(
                                self._db[collection][db_key]
                            )
                        if op == UpdateOp.INCREMENT:
                            self._db[collection][db_key]["value"] = (
                                self._db[collection][db_key]["value"] + arg
                            )
                        elif op == UpdateOp.APPEND:
                            self._db[collection][db_key]["value"] = (
                                self._db[collection][db_key]["value"] + arg
                            )
                        elif op == UpdateOp.PREPEND:
                            self._db[collection][db_key]["value"] = (
                                arg + self._db[collection][db_key]["value"]
                            )
                        else:
                            not_supported = True
                        if returning == "new":
                            return_value = get_value(
                                self._db[collection][db_key]
                            )
                    else:
                        if (
                            op == UpdateOp.INCREMENT
                            or op == UpdateOp.APPEND
                            or op == UpdateOp.PREPEND
                        ):
                            if collection not in self._db:
                                self._db[collection] = dict()
                            self._db[collection][db_key] = {
                                "value": arg,
                                "expiry": 0,
                            }
                        else:
                            not_supported = True
                if not_supported:
                    raise BadRequestError(
                        f"Update operation not supported {set}"
                    )
                self._db[collection][db_key]["etag"] = etag
                result = build_item(
                    id=db_key,
                    value=convert_value(return_value, self.__component__.type),
                    etag=etag,
                )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            db_key = get_db_key(op_parser, collection)
            where_etag = op_parser.get_where_etag()
            with self._lock:
                if where_etag is not None:
                    if not check_etag_lazy_evict(
                        collection, db_key, where_etag, True
                    ):
                        raise PreconditionFailedError
                if check_exists_lazy_evict(collection, db_key, True):
                    self._db[collection].pop(db_key)
                    if len(self._db[collection]) == 0:
                        self._db.pop(collection)
                else:
                    raise NotFoundError
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            raw_items = []
            if collection in self._db:
                for key in self._db[collection].keys():
                    if check_exists_lazy_evict(collection, key, False):
                        raw_items.append({"key": {"id": key}})
            filtered_items = QueryProcessor.query_items(
                raw_items,
                where=op_parser.get_where(),
                limit=op_parser.get_limit(),
                field_resolver=processor.resolve_root_field,
            )
            items = [
                build_item(id=fitem["key"]["id"]) for fitem in filtered_items
            ]
            result = KeyValueList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            raw_items = []
            if collection in self._db:
                for key in self._db[collection].keys():
                    if check_exists_lazy_evict(collection, key, False):
                        raw_items.append({"key": {"id": key}})
            result = QueryProcessor.count_items(
                raw_items,
                where=op_parser.get_where(),
                field_resolver=processor.resolve_root_field,
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = []
            op_parsers = op_parser.get_operation_parsers()
            for op_parser in op_parsers:
                collection = op_parser.get_collection_name() or collection
                if op_parser.op_equals(StoreOperation.PUT):
                    db_key = get_db_key(op_parser, collection)
                    value = op_parser.get_value()
                    etag = self._processor.generate_etag()
                    with self._lock:
                        if collection not in self._db:
                            self._db[collection] = dict()
                        self._db[collection][db_key] = {
                            "value": value,
                            "expiry": None,
                            "etag": etag,
                        }
                    result.append(build_item(id=db_key, etag=etag))
                elif op_parser.op_equals(StoreOperation.GET):
                    db_key = get_db_key(op_parser, collection)
                    if check_exists_lazy_evict(collection, db_key, False):
                        value = self._db[collection][db_key]["value"]
                        etag = self._db[collection][db_key]["etag"]
                    else:
                        value = None
                        etag = None
                    result.append(
                        build_item(
                            id=db_key,
                            value=convert_value(
                                value, self.__component__.type
                            ),
                            etag=etag,
                        )
                    )
                elif op_parser.op_equals(StoreOperation.DELETE):
                    db_key = get_db_key(op_parser, collection)
                    with self._lock:
                        if check_exists_lazy_evict(collection, db_key, True):
                            self._db[collection].pop(db_key)
                            if len(self._db[collection]) == 0:
                                self._db.pop(collection)
                    result.append(None)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            pass
        else:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        return Response(result=result)
