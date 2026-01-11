"""
Local Key Value Store on SQLite.
"""

__all__ = ["Local"]

from .sqlite import SQLite


class Local(SQLite):
    pass
