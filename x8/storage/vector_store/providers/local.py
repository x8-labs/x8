"""
Local Vector Store on Chroma.
"""

__all__ = ["Local"]

from .chroma import Chroma


class Local(Chroma):
    pass
