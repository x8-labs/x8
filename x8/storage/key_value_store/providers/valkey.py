"""
Key Value Store on Valkey.
"""

__all__ = ["RedisSimple"]

from .redis_simple import RedisSimple


class Valkey(RedisSimple):
    pass
