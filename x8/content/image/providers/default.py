"""
Default Image Provider.
"""

__all__ = ["Default"]

from .pillow import Pillow


class Default(Pillow):
    pass
