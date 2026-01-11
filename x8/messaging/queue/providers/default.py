"""
Default Queue.
"""

__all__ = ["Default"]


from .sqlite import SQLite


class Default(SQLite):
    pass
