"""
Local Key Value Store on SQLite.
"""

__all__ = ["Default"]

from .sqlite import SQLite


class Default(SQLite):
    pass
