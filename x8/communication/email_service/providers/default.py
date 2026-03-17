"""
Default Email Service Provider.
"""

__all__ = ["Default"]

from .sendgrid import SendGrid


class Default(SendGrid):
    pass
