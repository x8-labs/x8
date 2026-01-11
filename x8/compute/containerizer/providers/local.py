"""
Local provider for containerizer.
"""

__all__ = ["Local"]


from .docker import Docker


class Local(Docker):
    pass
