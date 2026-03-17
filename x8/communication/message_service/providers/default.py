"""
Default Message Service Provider.
"""

__all__ = ["Default"]

from .twilio import Twilio


class Default(Twilio):
    pass
