"""
Default provider for containerizer.
"""

__all__ = ["Default"]


from .docker import Docker


class Default(Docker):
    pass
