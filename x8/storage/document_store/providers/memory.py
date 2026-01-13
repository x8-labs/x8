"""
In Memory Document Store.
"""

from __future__ import annotations

__all__ = ["Memory"]

import copy
from threading import Lock
from typing import Any

from x8.core import Context, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import QueryProcessor
from x8.storage._common import CollectionResult, CollectionStatus, Index
from x8.storage._common import IndexHelper as BaseIndexHelper
from x8.storage._common import (
    IndexResult,
    IndexStatus,
    ItemProcessor,
    ParameterParser,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    Validator,
)

from .._helper import (
    build_item_from_value,
    build_query_result,
    get_collection_config,
)


class Memory(StoreProvider):
    collection: str | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None
    etag_embed_field: str | dict | None
    suppress_fields: list[str] | None
    nparams: dict[str, Any]

    # (collection, key, value)
    _db: dict[str, dict[str, Any]]
    _indexes: dict[str, dict[str, Index]]
    _collection_cache: dict[str, MemoryCollection]
    _lock: Lock

    def __init__(
        self,
        collection: str | None = None,
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = "pk",
        etag_embed_field: str | dict | None = "_etag",
        suppress_fields: list[str] | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            collection:
                Collection name.
            id_map_field:
                Field in the document to map into id.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            pk_map_field:
                Field in the document to map into pk.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            etag_embed_field:
                Field to store the generated ETAG value.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            suppress_fields:
                List of fields to supress when results are returned.
            nparams:
                Native parameters to the client. Not used.
        """
        self.collection = collection
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.etag_embed_field = etag_embed_field
        self.suppress_fields = suppress_fields
        self.nparams = nparams

        self._db = dict()
        self._indexes = dict()
        self._collection_cache = dict()
        self._lock = Lock()

    def __setup__(self, context: Context | None = None) -> None:
        pass

    def _get_collection_name(self, op_parser: StoreOperationParser) -> str:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        db_collection = (
            collection_name or self.collection or self.__component__.collection
        )
        if db_collection is None:
            raise BadRequestError("Collection name must be specified")
        return db_collection

    def _get_collections(
        self, op_parser: StoreOperationParser
    ) -> list[MemoryCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[MemoryCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(self._get_collections(single_op_parser))
            return collections
        db_collection = self._get_collection_name(op_parser)
        if db_collection in self._collection_cache:
            return [self._collection_cache[db_collection]]
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field,
            db_collection,
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field,
            db_collection,
        )
        etag_embed_field = ParameterParser.get_collection_parameter(
            self.etag_embed_field, db_collection
        )
        if db_collection not in self._db:
            self._db[db_collection] = dict()
        col = MemoryCollection(
            self._db[db_collection],
            id_map_field,
            pk_map_field,
            etag_embed_field,
            self.suppress_fields,
        )
        self._collection_cache[db_collection] = col
        return [col]

    def _validate(self, op_parser: StoreOperationParser):
        if op_parser.op_equals(StoreOperation.BATCH):
            Validator.validate_batch(
                op_parser.get_operation_parsers(),
                allowed_ops=[StoreOperation.PUT, StoreOperation.DELETE],
                single_collection=True,
            )

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        collections = self._get_collections(op_parser)
        if len(collections) == 1:
            processor = collections[0].processor
            data = collections[0].data
        result: Any = None

        def get_db_key_from_key(key):
            return tuple(
                sorted(processor.get_normalized_key_from_key(key).items())
            )

        def get_db_key_from_value(value):
            return tuple(
                sorted(
                    processor.get_normalized_key_from_value(document).items()
                )
            )

        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            collection_name = self._get_collection_name(op_parser)
            exists = op_parser.get_where_exists()
            config = get_collection_config(op_parser)
            with self._lock:
                conflict = False
                if collection_name not in self._db:
                    self._db[collection_name] = dict()
                else:
                    conflict = True
                index_results: list = []
                if config and config.indexes:
                    self._indexes[collection_name] = dict()
                    for index in config.indexes:
                        index_name = BaseIndexHelper.convert_index_name(index)
                        index.name = index_name
                        if index_name not in self._indexes[collection_name]:
                            self._indexes[collection_name][index_name] = index
                            index_results.append(
                                IndexResult(status=IndexStatus.CREATED)
                            )
                        else:
                            index_results.append(
                                IndexResult(
                                    status=IndexStatus.EXISTS,
                                    index=self._indexes[collection_name][
                                        index_name
                                    ],
                                )
                            )
                status: Any = CollectionStatus.CREATED
                if conflict:
                    if exists is False:
                        raise ConflictError
                    status = CollectionStatus.EXISTS
            result = CollectionResult(status=status, indexes=index_results)
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            collection_name = self._get_collection_name(op_parser)
            exists = op_parser.get_where_exists()
            status = CollectionStatus.DROPPED
            with self._lock:
                not_found = False
                if collection_name in self._db:
                    del self._db[collection_name]
                else:
                    not_found = True
                if collection_name in self._indexes:
                    del self._indexes[collection_name]
                if not_found:
                    if exists is True:
                        raise NotFoundError
                    status = CollectionStatus.NOT_EXISTS
            result = CollectionResult(status=status)
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = list(self._db.keys())
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            collection_name = self._get_collection_name(op_parser)
            result = collection_name in list(self._db.keys())
        # CREATE INDEX
        elif op_parser.op_equals(StoreOperation.CREATE_INDEX):
            collection_name = self._get_collection_name(op_parser)
            exists = op_parser.get_where_exists()
            index = op_parser.get_index()
            index_name = BaseIndexHelper.convert_index_name(index)
            index.name = index.name or index_name
            if collection_name not in self._indexes:
                self._indexes[collection_name] = dict()
            status = IndexStatus.CREATED
            match_index = None
            if index_name in self._indexes[collection_name]:
                if exists is False:
                    raise ConflictError
                status = IndexStatus.EXISTS
                match_index = self._indexes[collection_name][index_name]
            self._indexes[collection_name][index_name] = index
            result = IndexResult(status=status, index=match_index)
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            collection_name = self._get_collection_name(op_parser)
            exists = op_parser.get_where_exists()
            index_name = BaseIndexHelper.convert_index_name(
                op_parser.get_index()
            )
            status = IndexStatus.DROPPED
            if (
                collection_name in self._indexes
                and index_name in self._indexes[collection_name]
            ):
                del self._indexes[collection_name][index_name]
            else:
                if exists is True:
                    raise NotFoundError
                status = IndexStatus.NOT_EXISTS
            result = IndexResult(status=status)
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            collection_name = self._get_collection_name(op_parser)
            if collection_name in self._indexes:
                result = list(self._indexes[collection_name].values())
            else:
                result = []
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            key = op_parser.get_key()
            db_key = get_db_key_from_key(key)
            if db_key in data:
                value = data[db_key]
                result = build_item_from_value(
                    processor=processor, value=value, include_value=True
                )
            else:
                raise NotFoundError
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            document = op_parser.get_value()
            key = op_parser.get_key()
            where = op_parser.get_where()
            document = processor.add_embed_fields(document, key)
            exists = op_parser.get_where_exists()
            returning = op_parser.get_returning()
            db_key = get_db_key_from_value(document)
            with self._lock:
                if where is None:
                    data[db_key] = document
                elif exists is False:
                    if db_key in data:
                        raise PreconditionFailedError
                    data[db_key] = document
                elif exists is True:
                    if db_key not in data:
                        raise PreconditionFailedError
                    data[db_key] = document
                elif where is not None:
                    current_document = None
                    if db_key in data:
                        current_document = data[db_key]
                    if not QueryProcessor.eval_expr(
                        current_document, where, processor.resolve_field
                    ):
                        raise PreconditionFailedError
                    data[db_key] = document
            result = build_item_from_value(
                processor=processor,
                value=document,
                include_value=returning == "new",
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            key = op_parser.get_key()
            where = op_parser.get_where()
            returning = op_parser.get_returning()
            set = op_parser.get_set()
            db_key = get_db_key_from_key(key)
            uset = copy.deepcopy(set)
            if processor.needs_local_etag():
                etag = processor.generate_etag()
                uset = processor.add_etag_update(uset, etag)
            current_document = None
            updated_document = None
            with self._lock:
                if db_key in data:
                    current_document = data[db_key]
                if where is None:
                    if current_document is None:
                        raise NotFoundError
                else:
                    if not QueryProcessor.eval_expr(
                        current_document, where, processor.resolve_field
                    ):
                        raise PreconditionFailedError
                updated_document = QueryProcessor.update_item(
                    current_document, uset, processor.resolve_field
                )
                data[db_key] = updated_document
            if returning == "new":
                result = build_item_from_value(
                    processor=processor,
                    value=updated_document,
                    include_value=True,
                )
            elif returning == "old":
                result = build_item_from_value(
                    processor=processor,
                    value=current_document or {},
                    include_value=True,
                )
            else:
                result = build_item_from_value(
                    processor=processor,
                    value=updated_document,
                    include_value=False,
                )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            key = op_parser.get_key()
            where = op_parser.get_where()
            db_key = get_db_key_from_key(key)
            current_document = None
            with self._lock:
                if db_key in data:
                    current_document = data[db_key]
                if where is None:
                    if current_document is None:
                        raise NotFoundError
                else:
                    if not QueryProcessor.eval_expr(
                        current_document, where, processor.resolve_field
                    ):
                        raise PreconditionFailedError
                data.pop(db_key)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = list(data.values())
            nresult = QueryProcessor.query_items(
                items,
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
                field_resolver=processor.resolve_field,
            )
            items = [
                build_item_from_value(
                    processor=processor, value=item, include_value=True
                )
                for item in nresult
            ]
            result = build_query_result(items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            items = list(data.values())
            result = QueryProcessor.count_items(
                items,
                where=op_parser.get_where(),
                field_resolver=processor.resolve_field,
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = []
            op_parsers = op_parser.get_operation_parsers()
            for op_parser in op_parsers:
                if op_parser.op_equals(StoreOperation.PUT):
                    document = op_parser.get_value()
                    key = op_parser.get_key()
                    document = processor.add_embed_fields(document, key)
                    db_key = get_db_key_from_value(document)
                    with self._lock:
                        data[db_key] = document
                    result.append(
                        build_item_from_value(
                            processor=processor, value=document
                        )
                    )
                elif op_parser.op_equals(StoreOperation.DELETE):
                    key = op_parser.get_key()
                    db_key = get_db_key_from_key(key)
                    with self._lock:
                        if db_key in data:
                            data.pop(db_key)
                    result.append(None)
        # TRANSACT
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            result = []
            op_parsers = op_parser.get_operation_parsers()
            fail = False
            with self._lock:
                for i in range(0, len(op_parsers)):
                    op_parser = op_parsers[i]
                    processor = collections[i].processor
                    data = collections[i].data
                    if op_parser.op_equals(StoreOperation.PUT):
                        key = op_parser.get_key()
                        where = op_parser.get_where()
                        exists = op_parser.get_where_exists()
                        document = op_parser.get_value()
                        db_key = get_db_key_from_value(document)
                        if where is not None:
                            if exists is False and db_key in data:
                                fail = True
                                break
                            elif exists is True and db_key not in data:
                                fail = True
                                break
                            else:
                                current_document = None
                                if db_key in data:
                                    current_document = data[db_key]
                                if not QueryProcessor.eval_expr(
                                    current_document,
                                    where,
                                    processor.resolve_field,
                                ):
                                    fail = True
                                    break
                    elif op_parser.op_equals(
                        StoreOperation.UPDATE
                    ) or op_parser.op_equals(StoreOperation.DELETE):
                        key = op_parser.get_key()
                        where = op_parser.get_where()
                        db_key = get_db_key_from_key(key)
                        if db_key not in data:
                            fail = True
                            break
                        current_document = data[db_key]
                        if where is not None:
                            if not QueryProcessor.eval_expr(
                                current_document,
                                where,
                                processor.resolve_field,
                            ):
                                fail = True
                                break
                if fail:
                    raise ConflictError
                for i in range(0, len(op_parsers)):
                    op_parser = op_parsers[i]
                    processor = collections[i].processor
                    data = collections[i].data
                    if op_parser.op_equals(StoreOperation.PUT):
                        key = op_parser.get_key()
                        document = op_parser.get_value()
                        document = processor.add_embed_fields(document, key)
                        db_key = get_db_key_from_value(document)
                        data[db_key] = document
                        result.append(
                            build_item_from_value(
                                processor=processor, value=document
                            )
                        )
                    elif op_parser.op_equals(StoreOperation.UPDATE):
                        key = op_parser.get_key()
                        returning = op_parser.get_returning()
                        set = op_parser.get_set()
                        db_key = get_db_key_from_key(key)
                        uset = copy.deepcopy(set)
                        if processor.needs_local_etag():
                            etag = processor.generate_etag()
                            uset = processor.add_etag_update(uset, etag)
                        current_document = data[db_key]
                        updated_document = QueryProcessor.update_item(
                            current_document, uset, processor.resolve_field
                        )
                        data[db_key] = updated_document
                        if returning == "new":
                            result.append(
                                build_item_from_value(
                                    processor=processor,
                                    value=updated_document,
                                    include_value=True,
                                )
                            )
                        elif returning == "old":
                            result.append(
                                build_item_from_value(
                                    processor=processor,
                                    value=current_document,
                                    include_value=True,
                                )
                            )
                        else:
                            result.append(
                                build_item_from_value(
                                    processor=processor,
                                    value=updated_document,
                                    include_value=False,
                                )
                            )
                    elif op_parser.op_equals(StoreOperation.DELETE):
                        key = op_parser.get_key()
                        db_key = get_db_key_from_key(key)
                        data.pop(db_key)
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


class MemoryCollection:
    data: dict[str, Any]
    processor: ItemProcessor

    def __init__(
        self,
        data: dict[str, Any],
        id_map_field: str | None,
        pk_map_field: str | None,
        etag_embed_field: str | None,
        suppress_fields: list[str] | None,
    ):
        self.data = data
        self.processor = ItemProcessor(
            etag_embed_field=etag_embed_field,
            id_map_field=id_map_field,
            pk_map_field=pk_map_field,
            local_etag=True,
            suppress_fields=suppress_fields,
        )
