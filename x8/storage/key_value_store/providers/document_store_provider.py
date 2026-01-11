"""
Key Value Store on top of Document Store.
"""

from __future__ import annotations

__all__ = ["DocumentStoreProvider"]

import base64
from datetime import datetime, timezone
from typing import Any

from x8.core import Context, DataModel, NCall, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import (
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
from x8.storage.document_store import (
    AscIndex,
    CompositeIndex,
    DocumentBatch,
    DocumentCollectionConfig,
    DocumentItem,
    DocumentStore,
    DocumentStoreFeature,
)

from .._helper import (
    build_item,
    convert_value,
    get_collection_key,
    get_collection_name,
    get_collection_prefix,
    get_id,
)
from .._models import (
    KeyValueItem,
    KeyValueKeyType,
    KeyValueList,
    KeyValueValueType,
)


class KeyValueDocument(DataModel):
    id: str
    pk: str
    value: str | bytes | int | float
    encoded: bool
    expiry: float | None


class DocumentStoreProvider(StoreProvider):
    store: DocumentStore
    collection: str | None
    create_collection: bool

    _init: bool
    _ainit: bool
    _processor: ItemProcessor
    _op_converter: OperationConverter

    def __init__(
        self,
        store: DocumentStore,
        collection: str | None = None,
        create_collection: bool = True,
        **kwargs,
    ):
        """Initialize.

        Args:
            store:
                Document Store component.
            collection:
                Key value store collection.
            create_collection:
                A value indicating whether the document store
                collection should be created if it doesn't exist.
        """
        self.store = store
        self.collection = collection
        self.create_collection = create_collection

        self._init = False
        self._ainit = False
        self._client = None
        self._aclient = None
        self._processor = ItemProcessor()
        self._op_converter = OperationConverter(self._processor)

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self.store.__setup__(context=context)
        if self.create_collection:
            self.store.create_collection(config=self._get_collection_config())
        self._result_converter = ResultConverter(
            self._processor,
            self.__component__.type,
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        await self.store.__asetup__(context=context)
        if self.create_collection:
            await self.store.acreate_collection(
                config=self._get_collection_config()
            )
        self._result_converter = ResultConverter(
            self._processor,
            self.__component__.type,
        )
        self._ainit = True

    def _get_collection_config(self):
        if not self.store.__supports__(
            DocumentStoreFeature.QUERY_WITHOUT_INDEX
        ):
            return DocumentCollectionConfig(
                indexes=[
                    CompositeIndex(
                        fields=[
                            AscIndex(field="expiry", field_type="number"),
                            AscIndex(field="id", field_type="string"),
                        ]
                    )
                ]
            )
        return None

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall = self._get_operation_or_call(
            op_parser,
            ClientHelper(
                self.store,
                self._op_converter,
                self._result_converter,
            ),
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        if isinstance(ncall, NCall):
            nresult = ncall.invoke()
        else:
            nresult = self.store.__run__(
                operation=ncall,
                context=context,
                **kwargs,
            )
        result = self._convert_nresult(
            nresult,
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
        ncall = self._get_operation_or_call(
            op_parser,
            AsyncClientHelper(
                self.store,
                self._op_converter,
                self._result_converter,
            ),
        )
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        if isinstance(ncall, NCall):
            nresult = await ncall.ainvoke()
        else:
            nresult = await self.store.__arun__(
                operation=ncall,
                context=context,
                **kwargs,
            )
        result = self._convert_nresult(
            nresult,
            op_parser,
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_operation_or_call(
        self,
        op_parser: StoreOperationParser,
        helper: Any,
    ):
        collection: str | None = get_collection_name(self, op_parser)
        operation = None
        call = None
        op_converter = self._op_converter
        # EXISTS
        if op_parser.op_equals(StoreOperation.EXISTS):
            args: dict = {
                "key": op_parser.get_key(),
                "collection": collection,
            }
            call = NCall(helper.exists, args)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = {
                "key": op_parser.get_key(),
                "start": op_parser.get_start(),
                "end": op_parser.get_end(),
                "collection": collection,
            }
            call = NCall(helper.get, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            operation = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where(),
                op_parser.get_expiry(),
                op_parser.get_returning(),
                collection,
                self.store.__supports__(DocumentStoreFeature.TYPE_BINARY),
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args = {
                "key": op_parser.get_key(),
                "set": op_parser.get_set(),
                "where": op_parser.get_where(),
                "returning": op_parser.get_returning(),
                "collection": collection,
                "bytes_support": self.store.__supports__(
                    DocumentStoreFeature.TYPE_BINARY
                ),
            }
            call = NCall(helper.update, args)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            operation = self._op_converter.convert_delete(
                op_parser.get_key(),
                op_parser.get_where(),
                collection,
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            operation = op_converter.convert_query(
                op_parser.get_where(),
                op_parser.get_limit(),
                collection,
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            operation = op_converter.convert_count(
                op_parser.get_where(),
                collection,
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args = {
                "op_parsers": op_parser.get_operation_parsers(),
                "collection": collection,
                "bytes_support": self.store.__supports__(
                    DocumentStoreFeature.TYPE_BINARY
                ),
            }
            call = NCall(helper.batch, args)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            return StoreOperation.close()
        else:
            return None
        return operation or call

    def _convert_nresult(
        self,
        nresult: Any,
        op_parser: StoreOperationParser,
    ) -> Any:
        result: Any = None
        collection: str | None = get_collection_name(self, op_parser)
        result_converter = self._result_converter
        if op_parser.op_equals(StoreOperation.EXISTS):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            result = nresult
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = result_converter.convert_put(nresult.result, collection)
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(
                nresult.result, collection
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            result = result_converter.convert_query(nresult.result, collection)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = result_converter.convert_count(nresult.result)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = nresult
        return result


class OperationConverter:
    processor: ItemProcessor

    def __init__(self, processor: ItemProcessor, **kwargs: Any):
        self.processor = processor

    def convert_key_to_doc_id(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> str:
        return get_collection_key(
            collection,
            self.processor.get_id_from_key(key),
        )

    def convert_get(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> Operation:
        doc_id = self.convert_key_to_doc_id(key, collection)
        return StoreOperation.get(key=doc_id)

    def convert_put(
        self,
        key: KeyValueKeyType,
        value: KeyValueValueType,
        where: Expression | None,
        expiry: int | None,
        returning: str | None,
        collection: str | None,
        bytes_support: bool,
    ) -> Operation:
        doc_id = self.convert_key_to_doc_id(key, collection)
        doc_value = value
        doc_encoded = False
        if not bytes_support and isinstance(value, (bytes, memoryview)):
            doc_value = base64.b64encode(value).decode("utf-8")
            doc_encoded = True
        doc_expiry = expiry if expiry else None
        doc = KeyValueDocument(
            id=doc_id,
            pk=doc_id,
            value=doc_value,
            encoded=doc_encoded,
            expiry=doc_expiry,
        )
        return StoreOperation.put(value=doc, where=where, returning=returning)

    def convert_update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where: Expression | None,
        returning: str | None,
        collection: str | None,
    ) -> Operation:
        doc_id = self.convert_key_to_doc_id(key, collection)
        if len(set.operations) == 1 and (
            set.operations[0].field == Attribute.VALUE
            or set.operations[0].field == UpdateAttribute.VALUE
        ):
            op = set.operations[0].op
            if op == UpdateOp.INCREMENT:
                value = set.operations[0].args[0]
                update = Update().increment(Attribute.VALUE, value)
                return StoreOperation.update(
                    key=doc_id, set=update, where=where, returning=returning
                )
        raise BadRequestError(f"Update operation not supported: {set}")

    def convert_delete(
        self,
        key: KeyValueKeyType,
        where: Expression | None,
        collection: str | None,
    ) -> Operation:
        return StoreOperation.delete(
            key=self.convert_key_to_doc_id(key, collection),
            where=where,
        )

    def convert_query(
        self,
        where: Expression | None,
        limit: int | None,
        collection: str | None,
    ) -> Operation:
        return StoreOperation.query(
            select="id",
            where=self._convert_where(where, collection),
            limit=limit,
        )

    def convert_count(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> Operation:
        return StoreOperation.count(
            where=self._convert_where(where, collection),
        )

    def convert_batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
        bytes_support: bool,
    ) -> tuple[list[Operation], str]:
        def is_batch_op(
            op_parsers: list[StoreOperationParser], op: str
        ) -> bool:
            for op_parser in op_parsers:
                if not op_parser.op_equals(op):
                    return False
            return True

        operations: list = []
        if is_batch_op(op_parsers, StoreOperation.PUT):
            for op_parser in op_parsers:
                operations.append(
                    self.convert_put(
                        op_parser.get_key(),
                        op_parser.get_value(),
                        None,
                        None,
                        None,
                        op_parser.get_collection_name() or collection,
                        bytes_support,
                    )
                )
            op = "put"
        elif is_batch_op(op_parsers, StoreOperation.GET):
            for op_parser in op_parsers:
                operations.append(
                    self.convert_get(
                        op_parser.get_key(),
                        op_parser.get_collection_name() or collection,
                    )
                )
            op = "get"
        elif is_batch_op(op_parsers, StoreOperation.DELETE):
            for op_parser in op_parsers:
                operations.append(
                    self.convert_delete(
                        op_parser.get_key(),
                        op_parser.get_where(),
                        op_parser.get_collection_name() or collection,
                    )
                )
            op = "delete"
        else:
            raise BadRequestError("Batch operations not supported")
        return operations, op

    def convert_evict(
        self, item: DocumentItem, doc: KeyValueDocument
    ) -> Operation | None:
        current_time = datetime.now(timezone.utc).timestamp()
        has_expired = doc.expiry is not None and current_time > doc.expiry
        if not has_expired:
            return None
        etag = item.properties.etag if item.properties else None
        if etag:
            where = f"$etag='{etag}'"
        else:
            where = None
        return StoreOperation.delete(key=doc.id, where=where)

    def _convert_where(
        self,
        where: Expression | None,
        collection: str | None,
    ) -> str | None:
        current_time = datetime.now(timezone.utc).timestamp()
        expiry_check = f"(expiry = null or expiry > {current_time})"
        collection_prefix = get_collection_prefix(collection)
        if where is None and collection_prefix is None:
            return expiry_check
        if where is None:
            return f"starts_with(id, '{collection_prefix}') AND {expiry_check}"
        if isinstance(where, Function):
            if where.namespace == FunctionNamespace.BUILTIN:
                if where.name == QueryFunctionName.STARTS_WITH:
                    inp = get_collection_key(collection, where.args[1])
                    return f"starts_with(id, '{inp}') AND {expiry_check}"
                if where.name == QueryFunctionName.CONTAINS:
                    inp = get_collection_key(collection, where.args[1])
                    return f"contains(id, '{inp}') AND {expiry_check}"
        raise BadRequestError(f"Condition not supported: {where!r}")


class ResultConverter:
    processor: ItemProcessor
    type: str

    def __init__(self, processor: ItemProcessor, type: str, **kwargs: Any):
        self.processor = processor
        self.type = type

    def convert_value(self, value: Any) -> Any:
        return convert_value(value, self.type)

    def convert_range_value(
        self, value: Any, start: int | None, end: int | None
    ) -> Any:
        s = start if start else 0
        e = end + 1 if end else None
        return value[s:e]

    def convert_put(
        self, nresult: Any, collection: str | None
    ) -> KeyValueItem:
        id = get_id(nresult.key.id, collection)
        value = None
        if nresult.value:
            value = nresult.value["value"]
        return build_item(
            id=id,
            value=convert_value(value, self.type),
            etag=nresult.properties.etag,
        )

    def convert_update(
        self, nresult: Any, collection: str | None
    ) -> KeyValueItem:
        id = get_id(nresult.key.id, collection)
        value = None
        if nresult.value:
            value = nresult.value["value"]
        return build_item(
            id=id,
            value=convert_value(value, self.type),
            etag=nresult.properties.etag,
        )

    def convert_query(
        self, nresult: Any, collection: str | None
    ) -> KeyValueList:
        items = []
        for item in nresult.items:
            items.append(build_item(id=get_id(item.value["id"], collection)))
        return KeyValueList(items=items)

    def convert_count(
        self,
        nresult: Any,
    ) -> KeyValueList:
        return nresult


class ClientHelper:
    store: DocumentStore
    op_converter: OperationConverter
    result_converter: ResultConverter

    def __init__(
        self,
        store: DocumentStore,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
    ):
        self.store = store
        self.op_converter = op_converter
        self.result_converter = result_converter

    def exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> bool:
        get_operation = self.op_converter.convert_get(key, collection)
        try:
            response = self.store.__run__(get_operation)
        except NotFoundError:
            return False
        item = response.result
        doc = KeyValueDocument.from_dict(response.result.value)
        evict_operation = self.op_converter.convert_evict(item, doc)
        if evict_operation:
            try:
                self.store.__run__(evict_operation)
            except (NotFoundError, PreconditionFailedError):
                pass
            return False
        return True

    def get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> KeyValueItem:
        get_operation = self.op_converter.convert_get(key, collection)
        try:
            response = self.store.__run__(get_operation)
        except NotFoundError:
            raise NotFoundError
        item = response.result
        doc = KeyValueDocument.from_dict(response.result.value)
        evict_operation = self.op_converter.convert_evict(item, doc)
        if evict_operation:
            try:
                self.store.__run__(evict_operation)
            except (NotFoundError, PreconditionFailedError):
                pass

        doc_value = doc.value
        if doc.encoded:
            doc_value = base64.b64decode(str(doc_value).encode("utf-8"))
        return build_item(
            id=get_id(doc.id, collection),
            value=self.result_converter.convert_range_value(
                self.result_converter.convert_value(doc_value),
                start,
                end,
            ),
            etag=response.result.properties.etag,
        )

    def update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where: Expression | None,
        returning: str | None,
        collection: str | None,
        bytes_support: bool,
    ) -> Any:
        update_operation = self.op_converter.convert_update(
            key, set, where, returning, collection
        )
        try:
            return self.store.__run__(update_operation)
        except NotFoundError:
            put_operation = self.op_converter.convert_put(
                key=key,
                value=set.operations[0].args[0],
                where=where,
                expiry=None,
                returning=returning,
                collection=collection,
                bytes_support=bytes_support,
            )
            return self.store.__run__(put_operation)

    def batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
        bytes_support: bool,
    ) -> list[Any]:
        result: list = []
        operations, op = self.op_converter.convert_batch(
            op_parsers, collection, bytes_support
        )
        if op == "get":
            for operation in operations:
                response = self.store.__run__(operation)
                doc = KeyValueDocument.from_dict(response.result.value)
                doc_value = doc.value
                if doc.encoded:
                    doc_value = base64.b64decode(
                        str(doc_value).encode("utf-8")
                    )
                result.append(
                    build_item(
                        id=get_id(doc.id, collection),
                        value=self.result_converter.convert_value(doc_value),
                        etag=response.result.properties.etag,
                    )
                )
        elif op == "put":
            batch = DocumentBatch(operations=operations)
            response = self.store.__run__(StoreOperation.batch(batch=batch))
            for item in response.result:
                result.append(
                    build_item(
                        id=get_id(item.key.id, collection),
                        etag=item.properties.etag,
                    )
                )
        else:
            batch = DocumentBatch(operations=operations)
            response = self.store.__run__(StoreOperation.batch(batch=batch))
            for item in response.result:
                result.append(None)
        return result


class AsyncClientHelper:
    store: DocumentStore
    op_converter: OperationConverter
    result_converter: ResultConverter

    def __init__(
        self,
        store: DocumentStore,
        op_converter: OperationConverter,
        result_converter: ResultConverter,
    ):
        self.store = store
        self.op_converter = op_converter
        self.result_converter = result_converter

    async def exists(
        self,
        key: KeyValueKeyType,
        collection: str | None,
    ) -> bool:
        get_operation = self.op_converter.convert_get(key, collection)
        try:
            response = await self.store.__arun__(get_operation)
        except NotFoundError:
            return False
        item = response.result
        doc = KeyValueDocument.from_dict(response.result.value)
        evict_operation = self.op_converter.convert_evict(item, doc)
        if evict_operation:
            try:
                await self.store.__arun__(evict_operation)
            except (NotFoundError, PreconditionFailedError):
                pass
            return False
        return True

    async def get(
        self,
        key: KeyValueKeyType,
        start: int | None,
        end: int | None,
        collection: str | None,
    ) -> KeyValueItem:
        get_operation = self.op_converter.convert_get(key, collection)
        try:
            response = await self.store.__arun__(get_operation)
        except NotFoundError:
            raise NotFoundError
        item = response.result
        doc = KeyValueDocument.from_dict(response.result.value)
        evict_operation = self.op_converter.convert_evict(item, doc)
        if evict_operation:
            try:
                await self.store.__arun__(evict_operation)
            except (NotFoundError, PreconditionFailedError):
                pass

        doc_value = doc.value
        if doc.encoded:
            doc_value = base64.b64decode(str(doc_value).encode("utf-8"))
        return build_item(
            id=get_id(doc.id, collection),
            value=self.result_converter.convert_range_value(
                self.result_converter.convert_value(doc_value),
                start,
                end,
            ),
            etag=response.result.properties.etag,
        )

    async def update(
        self,
        key: KeyValueKeyType,
        set: Update,
        where: Expression | None,
        returning: str | None,
        collection: str | None,
        bytes_support: bool,
    ) -> Any:
        update_operation = self.op_converter.convert_update(
            key, set, where, returning, collection
        )
        try:
            return await self.store.__arun__(update_operation)
        except NotFoundError:
            put_operation = self.op_converter.convert_put(
                key=key,
                value=set.operations[0].args[0],
                where=where,
                expiry=None,
                returning=returning,
                collection=collection,
                bytes_support=bytes_support,
            )
            return await self.store.__arun__(put_operation)

    async def batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str | None,
        bytes_support: bool,
    ) -> list[Any]:
        result: list = []
        operations, op = self.op_converter.convert_batch(
            op_parsers, collection, bytes_support
        )
        if op == "get":
            import asyncio

            tasks = [
                self.store.__arun__(operation) for operation in operations
            ]
            responses = await asyncio.gather(*tasks)
            for response in responses:
                doc = KeyValueDocument.from_dict(response.result.value)
                doc_value = doc.value
                if doc.encoded:
                    doc_value = base64.b64decode(
                        str(doc_value).encode("utf-8")
                    )
                result.append(
                    build_item(
                        id=get_id(doc.id, collection),
                        value=self.result_converter.convert_value(doc_value),
                        etag=response.result.properties.etag,
                    )
                )
        elif op == "put":
            batch = DocumentBatch(operations=operations)
            response = await self.store.__arun__(
                StoreOperation.batch(batch=batch)
            )
            for item in response.result:
                result.append(
                    build_item(
                        id=get_id(item.key.id, collection),
                        etag=item.properties.etag,
                    )
                )
        else:
            batch = DocumentBatch(operations=operations)
            response = await self.store.__arun__(
                StoreOperation.batch(batch=batch)
            )
            for item in response.result:
                result.append(None)
        return result
