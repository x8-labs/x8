"""
Key Value Store on Dragonfly.
"""

__all__ = ["Dragonfly"]

from .redis_simple import RedisSimple


class Dragonfly(RedisSimple):
    pass
