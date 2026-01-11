"""
Object Store on File System.
"""

from __future__ import annotations

__all__ = ["FileSystem"]

import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import IO, Any
from urllib.parse import urljoin
from urllib.request import pathname2url

from x8.core import Context, DataModel, Operation, Response
from x8.core.exceptions import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    NotModified,
    PreconditionFailedError,
)
from x8.ql import (
    And,
    Comparison,
    ComparisonOp,
    Expression,
    Field,
    OrderBy,
    OrderByDirection,
    OrderByTerm,
    QueryFunction,
)
from x8.storage._common import (
    CollectionResult,
    CollectionStatus,
    MatchCondition,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
)
from x8.storage.document_store import DocumentStore, DocumentTransaction
from x8.storage.document_store.providers.sqlite import SQLite

from .._helper import (
    QueryArgs,
    get_collection_config,
    get_query_args,
    get_transfer_config,
)
from .._models import (
    ObjectCollectionConfig,
    ObjectItem,
    ObjectKey,
    ObjectList,
    ObjectProperties,
    ObjectTransferConfig,
    ObjectVersion,
)

DB_FILE_NAME = "__db"
CONFIG_ID = "config"
CONFIG_COLLECTION = "config"
OBJECT_COLLECTION = "object"
OBJECT_DOCUMENT_TYPE = "object"
VERSION_DOCUMENT_TYPE = "version"
PK = "#"


class ObjectDocument(DataModel):
    id: str
    pk: str
    object_id: str
    version: str | None
    metadata: dict | None
    properties: dict
    ts: float
    type: str


class ConfigDocument(DataModel):
    id: str
    pk: str
    versioned: bool


class FileSystem(StoreProvider):
    store_path: str
    folder: str | None
    db_file_name: str
    nparams: dict[str, Any]

    _init: bool
    _db_cache: dict[str, DocumentStore]

    def __init__(
        self,
        store_path: str = ".",
        folder: str | None = None,
        db_file_name: str = DB_FILE_NAME,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            store_path:
                Base store path. Defaults to ".".
            folder:
                Folder name mapped to object store collection name.
            db_file_name:
                File name of the SQLite DB that
                manages the metadata in each collection folder.
            nparams:
                Native parameters to file system operations.
        """
        self.store_path = store_path
        self.folder = folder
        self.nparams = nparams
        self.db_file_name = db_file_name
        self._init = False
        self._db_cache = dict()

    def _get_db(self, collection: str):
        if collection in self._db_cache:
            return self._db_cache[collection]
        collection_path = os.path.join(self.store_path, collection)
        db_path = os.path.join(collection_path, self.db_file_name)
        db = DocumentStore(
            collection=OBJECT_COLLECTION,
            __provider__=SQLite(
                database=db_path,
            ),
        )
        self._db_cache[collection] = db
        return db

    def _is_versioned(self, collection: str) -> bool:
        db = self._get_db(collection)
        try:
            res = db.get(key=CONFIG_ID, collection=CONFIG_COLLECTION)
            return res.result.value["versioned"]
        except NotFoundError:
            return False

    def __setup__(self, context: Context | None = None) -> None:
        if not self._init:
            return

        self._create_folder(self.store_path)
        self._init = True

    def _create_folder(self, folder: str) -> None:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

    def _get_folder_name(self, op_parser: StoreOperationParser) -> str:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        folder = (
            collection_name or self.folder or self.__component__.collection
        )
        if folder is None:
            raise BadRequestError("Collection name should be specified")
        return folder

    def _validate(self, op_parser: StoreOperationParser):
        pass

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            result = self.create_collection(
                collection=self._get_folder_name(op_parser),
                config=get_collection_config(op_parser),
                exists=op_parser.get_where_exists(),
            )
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            result = self.drop_collection(
                collection=self._get_folder_name(op_parser),
                exists=op_parser.get_where_exists(),
            )
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = self.list_collections()
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = self.has_collection(
                collection=self._get_folder_name(op_parser),
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = self.put(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value_as_bytes(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
                returning=op_parser.get_returning(),
                config=get_transfer_config(op_parser),
                collection=self._get_folder_name(op_parser),
            )
        # GET value
        elif op_parser.op_equals(StoreOperation.GET):
            result = self.get(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                file=op_parser.get_file(),
                stream=op_parser.get_stream(),
                match_condition=op_parser.get_match_condition(),
                start=op_parser.get_start(),
                end=op_parser.get_end(),
                config=get_transfer_config(op_parser),
                collection=self._get_folder_name(op_parser),
            )
        # GET metadata or properties
        elif op_parser.op_equals(
            StoreOperation.GET_METADATA
        ) or op_parser.op_equals(StoreOperation.GET_PROPERTIES):
            result = self.get_properties(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=op_parser.get_match_condition(),
                collection=self._get_folder_name(op_parser),
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            result = self.get_versions(
                id=op_parser.get_id_as_str(),
                collection=self._get_folder_name(op_parser),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = self.delete(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                match_condition=op_parser.get_match_condition(),
                collection=self._get_folder_name(op_parser),
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = self.update(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
                collection=self._get_folder_name(op_parser),
            )
        # COPY
        elif op_parser.op_equals(StoreOperation.COPY):
            result = self.copy(
                id=op_parser.get_id_as_str(),
                source_id=op_parser.get_source_id_as_str(),
                source_version=op_parser.get_source_version(),
                source_collection=op_parser.get_source_collection(),
                metadata=op_parser.get_metadata(),
                properties=op_parser.get_properties(),
                match_condition=op_parser.get_match_condition(),
                collection=self._get_folder_name(op_parser),
            )
        # GENERATE signed url
        elif op_parser.op_equals(StoreOperation.GENERATE):
            result = self.generate(
                id=op_parser.get_id_as_str(),
                version=op_parser.get_version(),
                method=op_parser.get_method(),
                expiry=op_parser.get_expiry_in_seconds(),
                collection=self._get_folder_name(op_parser),
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            query_args = get_query_args(op_parser)
            result = self.query(
                query_args=query_args,
                collection=self._get_folder_name(op_parser),
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            query_args = get_query_args(op_parser)
            result = self.count(
                query_args=query_args,
                collection=self._get_folder_name(op_parser),
            )
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = self.batch(
                op_parser.get_operation_parsers(),
                collection=self._get_folder_name(op_parser),
            )
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            result = self.close()
        return Response(result=result)

    def create_collection(
        self,
        collection: str,
        config: ObjectCollectionConfig | None,
        exists: bool | None,
    ) -> CollectionResult:
        db = self._get_db(collection)
        folder_path = os.path.join(self.store_path, collection)
        if os.path.isdir(folder_path):
            if exists is False:
                raise ConflictError
            else:
                return CollectionResult(status=CollectionStatus.EXISTS)
        self._create_folder(folder_path)
        db.create_collection()
        db.create_collection(CONFIG_COLLECTION)
        versioned = False
        if config is not None and config.versioned:
            versioned = True
        db.put(
            ConfigDocument(id=CONFIG_ID, pk=CONFIG_ID, versioned=versioned),
            collection=CONFIG_COLLECTION,
        )
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        collection: str,
        exists: bool | None,
    ) -> CollectionResult:
        folder_path = os.path.join(self.store_path, collection)
        if not os.path.isdir(folder_path):
            if exists is True:
                raise NotFoundError
            else:
                return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        db = self._get_db(collection)
        db.close()
        if collection in self._db_cache:
            self._db_cache.pop(collection)
        import shutil

        shutil.rmtree(folder_path)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self) -> list[str]:
        result = []
        items = os.listdir(self.store_path)
        for item in items:
            full_path = os.path.join(self.store_path, item)
            if os.path.isdir(full_path):
                result.append(item)
        return result

    def has_collection(self, collection: str) -> bool:
        folder_path = os.path.join(self.store_path, collection)
        return os.path.isdir(folder_path)

    def put(
        self,
        id: str,
        value: bytes | None,
        file: str | None,
        stream: IO | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        returning: str | None,
        config: ObjectTransferConfig | None,
        collection: str,
    ) -> ObjectItem:
        db = self._get_db(collection)
        versioned = self._is_versioned(collection)
        version = None
        link_path = None
        try:
            current_item = self._db_get(db, id, version)
        except NotFoundError:
            current_item = None
        self._match(current_item, match_condition)

        if versioned:
            version = str(uuid.uuid4())
        object_path, link_path = self._convert_object_link_path(
            collection, id, version
        )
        os.makedirs(os.path.dirname(object_path), exist_ok=True)
        if file:
            shutil.copy(file, object_path)
        elif value:
            with open(object_path, "wb") as f:
                f.write(value)
        elif stream:
            with open(object_path, "wb") as f:
                shutil.copyfileobj(stream, f)

        if link_path:
            os.makedirs(os.path.dirname(link_path), exist_ok=True)
            if os.path.islink(link_path):
                os.unlink(link_path)
            os.symlink(object_path, link_path)

        last_modified = datetime.now(timezone.utc).timestamp()
        content_length = os.path.getsize(object_path)
        ts = datetime.now(timezone.utc).timestamp()
        if properties is None:
            props = dict()
        else:
            props = properties.copy()
        props["last_modified"] = last_modified
        props["content_length"] = content_length
        if version:
            transaction = DocumentTransaction()
            transaction.put(
                value=ObjectDocument(
                    id=self._get_db_object_id(id),
                    pk=PK,
                    object_id=id,
                    version=version,
                    metadata=metadata,
                    properties=props,
                    ts=ts,
                    type=OBJECT_DOCUMENT_TYPE,
                )
            )
            transaction.put(
                value=ObjectDocument(
                    id=self._get_db_version_id(id, version),
                    pk=PK,
                    object_id=id,
                    version=version,
                    metadata=metadata,
                    properties=props,
                    ts=ts,
                    type=VERSION_DOCUMENT_TYPE,
                )
            )
            res = db.transact(transaction=transaction)
            etag = res.result[1].properties.etag
        else:
            res = db.put(
                ObjectDocument(
                    id=self._get_db_object_id(id),
                    pk=PK,
                    object_id=id,
                    version=version,
                    metadata=metadata,
                    properties=props,
                    ts=ts,
                    type=OBJECT_DOCUMENT_TYPE,
                )
            )
            etag = res.result.properties.etag
        return_value = None
        if returning == "new":
            return_value = value
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            value=return_value,
            metadata=metadata,
            properties=ObjectProperties(
                etag=etag,
                last_modified=last_modified,
                content_length=content_length,
            ),
            url=self._convert_url(link_path or object_path),
        )

    def get(
        self,
        id: str,
        version: str | None,
        file: str | None,
        stream: IO | None,
        match_condition: MatchCondition,
        start: int | None,
        end: int | None,
        config: ObjectTransferConfig | None,
        collection: str,
    ) -> ObjectItem:
        db = self._get_db(collection)
        value = None
        object_path, link_path = self._convert_object_link_path(
            collection, id, version
        )

        if not os.path.isfile(object_path) and not os.path.islink(object_path):
            raise NotFoundError

        item = self._db_get(db, id, version)
        self._match(item, match_condition)

        with open(object_path, "rb") as f:
            if start is not None or end is not None:
                f.seek(start or 0)
                data = f.read(
                    (end - (start or 0) + 1) if end is not None else None
                )
            else:
                data = f.read()
        if file:
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as out_file:
                out_file.write(data)
        elif stream:
            stream.write(data)
            stream.seek(0)
        else:
            value = data

        return ObjectItem(
            key=ObjectKey(id=id, version=item.version),
            value=value,
            metadata=item.metadata,
            properties=ObjectProperties.from_dict(item.properties),
            url=self._convert_url(link_path or object_path),
        )

    def get_properties(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        collection: str,
    ) -> ObjectItem:
        db = self._get_db(collection)
        object_path, link_path = self._convert_object_link_path(
            collection, id, version
        )
        try:
            item = self._db_get(db, id, version)
            self._match(item, match_condition)
        except NotFoundError:
            raise NotFoundError
        return ObjectItem(
            key=ObjectKey(id=id, version=item.version),
            metadata=item.metadata,
            properties=ObjectProperties.from_dict(item.properties),
            url=self._convert_url(link_path or object_path),
        )

    def get_versions(self, id: str, collection: str) -> ObjectItem:
        db = self._get_db(collection)
        object_path, link_path = self._convert_object_link_path(
            collection, id, None
        )
        res = db.query(
            where=And(
                lexpr=Comparison(
                    lexpr=Field(path="object_id"),
                    op=ComparisonOp.EQ,
                    rexpr=id,
                ),
                rexpr=Comparison(
                    lexpr=Field(path="type"),
                    op=ComparisonOp.EQ,
                    rexpr=VERSION_DOCUMENT_TYPE,
                ),
            ),
            order_by=OrderBy(
                terms=[
                    OrderByTerm(
                        field="ts",
                        direction=OrderByDirection.DESC,
                    )
                ]
            ),
        )

        versions: list[ObjectVersion] = []
        for item in res.result.items:
            obj = ObjectDocument.from_dict(item.value)
            properties = ObjectProperties.from_dict(obj.properties)
            properties.etag = item.properties.etag
            versions.append(
                ObjectVersion(
                    version=obj.version,
                    properties=properties,
                    latest=False,
                )
            )
        if len(versions) == 0:
            raise NotFoundError
        versions[0].latest = True
        return ObjectItem(
            key=ObjectKey(id=id),
            versions=versions[::-1],
            url=self._convert_url(link_path or object_path),
        )

    def delete(
        self,
        id: str,
        version: str | None,
        match_condition: MatchCondition,
        collection: str,
    ):
        db = self._get_db(collection)
        versioned = self._is_versioned(collection)
        try:
            current_item = self._db_get(db, id, version)
        except NotFoundError:
            current_item = None
        self._match(current_item, match_condition)
        if versioned:
            try:
                obj_res = db.get(key=self._get_db_object_key(id))
            except NotFoundError:
                raise NotFoundError
            if (
                version == "*"
                or version is None
                or version == obj_res.result.value["version"]
            ):
                res = db.query(
                    where=And(
                        lexpr=Comparison(
                            lexpr=Field(path="object_id"),
                            op=ComparisonOp.EQ,
                            rexpr=id,
                        ),
                        rexpr=Comparison(
                            lexpr=Field(path="type"),
                            op=ComparisonOp.EQ,
                            rexpr=VERSION_DOCUMENT_TYPE,
                        ),
                    )
                )
                for item in res.result.items:
                    obj = ObjectDocument.from_dict(item.value)
                    object_path, link_path = self._convert_object_link_path(
                        collection, obj.object_id, obj.version
                    )
                    if os.path.isfile(object_path) or os.path.islink(
                        object_path
                    ):
                        self._delete_file(object_path)
                    if link_path and os.path.islink(link_path):
                        self._delete_file(link_path)
                    db.delete(key=item.key)
                db.delete(key=obj_res.result.key)
            else:
                res = db.get(key=self._get_db_version_key(id, version))
                obj = ObjectDocument.from_dict(res.result.value)
                object_path, link_path = self._convert_object_link_path(
                    collection, obj.object_id, obj.version
                )
                self._delete_file(object_path)
                if link_path and os.path.islink(link_path):
                    self._delete_file(link_path)
                db.delete(key=res.result.key)
        else:
            try:
                res = db.get(key=self._get_db_object_key(id))
            except NotFoundError:
                raise NotFoundError
            obj = ObjectDocument.from_dict(res.result.value)
            object_path, link_path = self._convert_object_link_path(
                collection, obj.object_id, obj.version
            )
            self._delete_file(object_path)
            db.delete(key=res.result.key)
        return None

    def update(
        self,
        id: str,
        version: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        collection: str,
    ) -> ObjectItem:
        db = self._get_db(collection)
        object_path, link_path = self._convert_object_link_path(
            collection, id, version
        )
        versioned = self._is_versioned(collection)
        last_modified = datetime.now(timezone.utc).timestamp()
        try:
            obj_res = db.get(key=self._get_db_object_key(id))
        except NotFoundError:
            raise NotFoundError
        obj = ObjectDocument.from_dict(obj_res.result.value)
        if versioned:
            if obj.version and (version is None or version == obj.version):
                ver_res = db.get(key=self._get_db_version_key(id, obj.version))
                ver = ObjectDocument.from_dict(ver_res.result.value)
                ver.properties["etag"] = ver_res.result.properties.etag
                self._match(ver, match_condition)
                ver.properties.pop("etag")
                obj.properties["last_modified"] = last_modified
                obj.properties = obj.properties | (properties or {})
                ver.properties["last_modified"] = last_modified
                ver.properties = ver.properties | (properties or {})
                if metadata:
                    obj.metadata = metadata
                    ver.metadata = metadata
                transaction = DocumentTransaction()
                transaction.put(obj)
                transaction.put(ver)
                res = db.transact(transaction=transaction)
                etag = res.result[1].properties.etag
                item = obj
            elif version is not None:
                ver_res = db.get(key=self._get_db_version_key(id, version))
                ver = ObjectDocument.from_dict(ver_res.result.value)
                ver.properties["etag"] = ver_res.result.properties.etag
                self._match(ver, match_condition)
                ver.properties.pop("etag")
                ver.properties["last_modified"] = last_modified
                ver.properties = ver.properties | (properties or {})
                if metadata:
                    ver.metadata = metadata
                res = db.put(ver)
                etag = res.result.properties.etag
                item = ver
        else:
            obj.properties["etag"] = obj_res.result.properties.etag
            self._match(obj, match_condition)
            obj.properties.pop("etag")
            obj.properties["last_modified"] = last_modified
            obj.properties = obj.properties | (properties or {})
            if metadata:
                obj.metadata = metadata
            res = db.put(obj)
            etag = res.result.properties.etag
            item = obj
        item.properties["etag"] = etag
        return ObjectItem(
            key=ObjectKey(id=id, version=item.version),
            metadata=item.metadata,
            properties=ObjectProperties.from_dict(item.properties),
            url=self._convert_url(link_path or object_path),
        )

    def copy(
        self,
        id: str,
        source_id: str,
        source_version: str | None,
        source_collection: str | None,
        metadata: dict | None,
        properties: dict | None,
        match_condition: MatchCondition,
        collection: str,
    ) -> ObjectItem:
        source_collection = source_collection or collection
        source_db = self._get_db(source_collection)
        dest_db = self._get_db(collection)

        source_object_path, source_link_path = self._convert_object_link_path(
            source_collection,
            source_id,
            source_version,
        )

        if not os.path.isfile(source_object_path) and not os.path.islink(
            source_object_path
        ):
            raise NotFoundError
        source_item = self._db_get(
            source_db,
            source_id,
            source_version,
        )
        dest_item = None
        try:
            dest_item = self._db_get(dest_db, id, None)
        except NotFoundError:
            pass
        self._match(dest_item, match_condition)
        new_properties = source_item.properties | (properties or {})
        new_properties.pop("etag")
        new_properties.pop("last_modified")
        if source_item.metadata or metadata:
            new_metadata = (source_item.metadata or {}) | (metadata or {})
        else:
            new_metadata = None
        return self.put(
            id=id,
            value=None,
            file=source_object_path,
            stream=None,
            metadata=new_metadata,
            properties=new_properties,
            match_condition=match_condition,
            returning=None,
            config=None,
            collection=collection,
        )

    def generate(
        self,
        id: str,
        version: str | None,
        method: str | None,
        expiry: int | None,
        collection: str,
    ) -> ObjectItem:
        object_path, link_path = self._convert_object_link_path(
            collection, id, version
        )
        return ObjectItem(
            key=ObjectKey(id=id, version=version),
            url=self._convert_url(link_path or object_path),
        )

    def query(
        self,
        query_args: QueryArgs,
        collection: str,
    ) -> ObjectList:
        db = self._get_db(collection)
        where: Expression = Comparison(
            lexpr=Field(path="type"),
            op=ComparisonOp.EQ,
            rexpr=OBJECT_DOCUMENT_TYPE,
        )
        if query_args.prefix:
            where = And(
                lexpr=where,
                rexpr=QueryFunction.starts_with(
                    field="object_id",
                    value=query_args.prefix,
                ),
            )
        if query_args.continuation:
            where = And(
                lexpr=where,
                rexpr=Comparison(
                    lexpr=Field(path="object_id"),
                    op=ComparisonOp.GT,
                    rexpr=query_args.continuation,
                ),
            )
        limit = query_args.limit
        if query_args.paging and query_args.page_size:
            if limit:
                if query_args.page_size < limit:
                    limit = query_args.page_size
            else:
                limit = query_args.page_size

        items: list[ObjectItem] = []
        prefixes: list[str] = []
        continuation = None
        res = db.query(
            where=where,
            order_by=OrderBy(
                terms=[
                    OrderByTerm(
                        field="object_id",
                        direction=OrderByDirection.ASC,
                    )
                ]
            ),
            limit=limit,
        )
        for item in res.result.items:
            obj = ObjectDocument.from_dict(item.value)
            continuation = obj.object_id
            if query_args.delimiter:
                relative_path = obj.object_id
                if query_args.prefix:
                    start = len(query_args.prefix)
                    relative_path = relative_path[start:]
                prefix_part, _, rest = relative_path.partition(
                    query_args.delimiter
                )
                if rest:
                    prefix = (
                        (query_args.prefix or "")
                        + prefix_part
                        + query_args.delimiter
                    )
                    if prefix not in prefixes:
                        prefixes.append(prefix)
                    continue
            object_path, link_path = self._convert_object_link_path(
                collection, obj.object_id, obj.version
            )
            items.append(
                ObjectItem(
                    key=ObjectKey(id=obj.object_id, version=obj.version),
                    url=self._convert_url(link_path or object_path),
                )
            )
        if not query_args.paging:
            continuation = None
        if limit:
            if len(res.result.items) < limit:
                continuation = None
        return ObjectList(
            items=items,
            prefixes=sorted(prefixes),
            continuation=continuation,
        )

    def count(
        self,
        query_args: QueryArgs,
        collection: str,
    ) -> int:
        res = self.query(query_args, collection)
        count = len(res.items)
        if res.prefixes:
            count = count + len(res.prefixes)
        return count

    def batch(
        self,
        op_parsers: list[StoreOperationParser],
        collection: str,
    ) -> list[Any]:
        result: list = []
        for op_parser in op_parsers:
            if op_parser.op_equals(StoreOperation.DELETE):
                id = op_parser.get_id_as_str()
                version = op_parser.get_version()
                match_condition = op_parser.get_match_condition()
                self.delete(id, version, match_condition, collection)
                result.append(None)
        return result

    def close(self) -> Any:
        pass

    def _convert_url(self, file_path: str) -> str:
        absolute_path = os.path.abspath(file_path)
        url_path = pathname2url(absolute_path)
        return urljoin("file://", url_path)

    def _convert_object_link_path(
        self, collection: str, id: str, version: str | None
    ) -> tuple[str, str | None]:
        link_path = None
        if version:
            link_path = os.path.join(
                self.store_path,
                collection,
                id.lstrip("/"),
            )
            object_path = os.path.join(
                self.store_path,
                collection,
                version,
                id.lstrip("/"),
            )
        else:
            object_path = os.path.join(
                self.store_path,
                collection,
                id.lstrip("/"),
            )
        object_path = os.path.normpath(object_path)
        if link_path is not None:
            link_path = os.path.normpath(link_path)
        return object_path, link_path

    def _db_get(
        self,
        db: DocumentStore,
        id: str,
        version: str | None,
    ) -> ObjectDocument:
        try:
            get_res = db.get(
                key=(
                    self._get_db_object_key(id)
                    if version is None
                    else self._get_db_version_key(id, version)
                )
            )
            item = ObjectDocument.from_dict(get_res.result.value)
            if item.version:
                get_res = db.get(
                    key=(self._get_db_version_key(id, item.version))
                )
                item = ObjectDocument.from_dict(get_res.result.value)
            etag = (
                get_res.result.properties.etag
                if get_res.result.properties is not None
                else None
            )
            item.properties["etag"] = etag
            return item
        except NotFoundError:
            raise NotFoundError

    def _match(
        self, item: ObjectDocument | None, match_condition: MatchCondition
    ):
        if item is None:
            if (
                match_condition.if_match
                or match_condition.if_unmodified_since
                or match_condition.exists is True
            ):
                raise PreconditionFailedError
            return

        if match_condition.exists is False:
            raise PreconditionFailedError

        etag = item.properties["etag"]
        last_modified = item.properties["last_modified"]
        version = item.version
        if match_condition.if_match is not None:
            if (
                match_condition.if_match != etag
                and match_condition.if_match != "*"
            ):
                raise PreconditionFailedError
        if match_condition.if_none_match is not None:
            etag_list = [
                etag.strip()
                for etag in match_condition.if_none_match.split(",")
            ]
            if etag in etag_list:
                raise NotModified
        if match_condition.if_version_match is not None:
            if match_condition.if_version_match != version:
                raise PreconditionFailedError
        if match_condition.if_version_not_match is not None:
            if match_condition.if_version_not_match == version:
                raise PreconditionFailedError
        if match_condition.if_modified_since is not None:
            if last_modified <= match_condition.if_modified_since:
                raise NotModified
        if match_condition.if_unmodified_since is not None:
            if last_modified > match_condition.if_unmodified_since:
                raise PreconditionFailedError

    def _delete_file(self, path):
        os.remove(path)
        folder = os.path.dirname(path)
        store_abs_path = os.path.abspath(self.store_path)
        while folder and os.path.abspath(folder) != store_abs_path:
            if os.path.isdir(folder):
                try:
                    os.rmdir(folder)
                    folder = os.path.dirname(folder)
                except OSError:
                    break
            else:
                break

    def _get_db_object_key(self, id: str) -> dict:
        return {"id": self._get_db_object_id(id), "pk": PK}

    def _get_db_version_key(self, id: str, version: str) -> dict:
        return {"id": self._get_db_version_id(id, version), "pk": PK}

    def _get_db_object_id(self, id: str) -> str:
        return f"{OBJECT_DOCUMENT_TYPE}-{id}"

    def _get_db_version_id(self, id: str, version: str) -> str:
        return f"{VERSION_DOCUMENT_TYPE}-{id}-{version}"
