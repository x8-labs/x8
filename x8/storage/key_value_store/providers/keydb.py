"""
Key Value Store on KeyDB.
"""

__all__ = ["KeyDB"]

from .redis_simple import RedisSimple


class KeyDB(RedisSimple):
    pass
