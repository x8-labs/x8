"""
Config Store on top of Document Store.
"""

from __future__ import annotations

__all__ = ["DocumentStoreProvider"]

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from x8.core import Context, Operation, Response
from x8.ql import (
    And,
    Comparison,
    ComparisonOp,
    Expression,
    Field,
    QueryFunction,
)
from x8.storage._common import (
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)
from x8.storage.document_store import (
    AscIndex,
    CompositeIndex,
    DocumentCollectionConfig,
    DocumentStore,
    HashIndex,
)

from .._helper import QueryArgs, get_query_args, normalize_label
from .._models import ConfigItem, ConfigKey, ConfigList, ConfigProperties


@dataclass
class ConfigDocument:
    id: str
    pk: str
    tenant_id: str
    config_id: str
    label: str
    value: str
    metadata: dict | None
    updated_time: float

    def to_dict(self):
        return self.__dict__


class DocumentStoreProvider(StoreProvider):
    store: DocumentStore
    tenant_id: str
    create_collection: bool
    create_indexes: bool

    _init: bool
    _ainit: bool
    _processor: ItemProcessor
    _converter: OperationConverter

    def __init__(
        self,
        store: DocumentStore,
        tenant_id: str = "default",
        create_collection: bool = True,
        create_indexes: bool = True,
        **kwargs,
    ):
        """Initialize.

        Args:
            store:
                Document Store component.
            tenant_id:
                Tenant id for multi-tenancy, defaults to "default".
            create_collection:
                A value indicating whether the collection should be
                created if it doesn't exist.
            create_indexes:
                A value indicating whether indexes should be
                created as part of the collection creation.
        """
        self.store = store
        self.tenant_id = tenant_id
        self.create_collection = create_collection
        self.create_indexes = create_indexes

        self._init = False
        self._ainit = False
        self._client = None
        self._aclient = None
        self._processor = ItemProcessor()
        self._converter = OperationConverter(self.tenant_id)

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self.store.__setup__(context=context)
        if self.create_collection:
            self.store.create_collection(config=self._get_collection_config())
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return
        await self.store.__asetup__(context=context)
        if self.create_collection:
            await self.store.acreate_collection(
                config=self._get_collection_config()
            )
        self._ainit = True

    def _get_collection_config(self):
        if self.create_indexes:
            return DocumentCollectionConfig(
                indexes=[
                    CompositeIndex(
                        fields=[
                            HashIndex(field="pk", field_type="string"),
                            HashIndex(field="label", field_type="string"),
                            AscIndex(field="config_id", field_type="string"),
                        ]
                    ),
                    CompositeIndex(
                        fields=[
                            HashIndex(field="pk", field_type="string"),
                            AscIndex(field="config_id", field_type="string"),
                        ]
                    ),
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
        ncall = self._get_operation(op_parser, self._converter)
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = self.store.__run__(operation=ncall, context=context)
        result = self._convert_nresult(nresult, op_parser, self._processor)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = self.get_op_parser(operation)
        ncall = self._get_operation(op_parser, self._converter)
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await self.store.__arun__(operation=ncall, context=context)
        result = self._convert_nresult(nresult, op_parser, self._processor)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_operation(
        self, op_parser: StoreOperationParser, converter: OperationConverter
    ):
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            return StoreOperation.get(key=converter.convert_key(id, label))
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            value = op_parser.get_value()
            label = normalize_label(op_parser.get_label())
            metadata = op_parser.get_metadata()
            return StoreOperation.put(
                value=ConfigDocument(
                    id=converter.convert_id(id, label),
                    pk=converter.convert_pk(),
                    tenant_id=self.tenant_id,
                    config_id=id,
                    label=label,
                    value=value,
                    metadata=metadata,
                    updated_time=datetime.now(timezone.utc).timestamp(),
                ).to_dict()
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            return StoreOperation.delete(key=converter.convert_key(id, label))
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            where = converter.convert_filter(get_query_args(op_parser))
            return StoreOperation.query(where=where)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            where = converter.convert_filter(get_query_args(op_parser))
            return StoreOperation.count(where=where)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            return StoreOperation.close()
        else:
            return None

    def _convert_nresult(
        self,
        nresult: Any,
        op_parser: StoreOperationParser,
        processor: ItemProcessor,
    ) -> Any:
        result: Any = None
        nresult = nresult.result

        def convert_item(item):
            return ConfigItem(
                key=ConfigKey(
                    id=item.value["config_id"], label=item.value["label"]
                ),
                value=item.value["value"],
                metadata=item.value["metadata"],
                properties=ConfigProperties(
                    updated_time=item.value["updated_time"]
                ),
            )

        # GET
        if op_parser.op_equals(StoreOperation.GET):
            result = convert_item(nresult)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = ConfigItem(
                key=ConfigKey(
                    id=op_parser.get_id_as_str(),
                    label=normalize_label(op_parser.get_label()),
                ),
                value=op_parser.get_value(),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult.items:
                items.append(convert_item(item))
            items = sorted(items, key=lambda x: (x.key.label, x.key.id))
            result = ConfigList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = nresult
        return result


class OperationConverter:
    tenant_id: str

    def __init__(self, tenant_id: str, **kwargs: Any):
        self.tenant_id = tenant_id

    def convert_id(self, id: str, label: str):
        return f"{self.tenant_id}:{id.replace('/', '$')}:{label}"

    def convert_pk(self):
        return self.tenant_id

    def convert_key(self, id: str, label: str):
        key = dict()
        key["$id"] = self.convert_id(id, label)
        key["$pk"] = self.convert_pk()
        return key

    def convert_filter(self, query_args: QueryArgs) -> Expression:
        where: Expression = Comparison(
            lexpr=Field(path="$pk"),
            op=ComparisonOp.EQ,
            rexpr=self.tenant_id,
        )
        if query_args.label_filter is not None:
            where = And(
                lexpr=where,
                rexpr=Comparison(
                    lexpr=Field(path="label"),
                    op=ComparisonOp.EQ,
                    rexpr=query_args.label_filter,
                ),
            )
        if query_args.id_filter:
            where = And(
                lexpr=where,
                rexpr=QueryFunction.starts_with(
                    "config_id",
                    query_args.id_filter,
                ),
            )
        return where
