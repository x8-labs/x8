import os

from x8.core import Provider


class SQLiteProvider(Provider):
    def _init_folder(self, database: str):
        if database != ":memory:" and not database.startswith("file:"):
            db_path = os.path.abspath(os.path.expanduser(database))
            db_folder = os.path.dirname(db_path)
            if db_folder:
                os.makedirs(db_folder, exist_ok=True)
            self.database = db_path
