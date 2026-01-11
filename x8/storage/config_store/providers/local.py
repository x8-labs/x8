"""
Local Config Store on SQLite.
"""

__all__ = ["Local"]


from x8.core.exceptions import ConflictError
from x8.storage.document_store import DocumentStore
from x8.storage.document_store.providers.sqlite import SQLite

from .document_store_provider import DocumentStoreProvider


class Local(DocumentStoreProvider):
    database: str
    table: str

    def __init__(
        self,
        database: str = ":memory:",
        table: str = "config",
        **kwargs,
    ):
        """Intialize.

        Args:
            database:
                SQLite database, defaults to ":memory:".
            table:
                SQLite table name, defaults to "config".
        """
        self.database = database
        self.table = table
        store = DocumentStore(
            __provider__=SQLite(database=self.database, table=self.table),
            collection=table,
        )
        try:
            store.create_collection()
        except ConflictError:
            pass
        super().__init__(store=store)
