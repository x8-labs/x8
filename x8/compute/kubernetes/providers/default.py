"""
Default kubernetes deployment.
"""

__all__ = ["Default"]


from .local import Local


class Default(Local):
    pass
