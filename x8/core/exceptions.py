__all__ = [
    "BaseError",
    "BadRequestError",
    "ConflictError",
    "ForbiddenError",
    "InternalError",
    "LoadError",
    "NotFoundError",
    "NotModified",
    "NotSupportedError",
    "PreconditionFailedError",
    "SpecError",
    "UnauthorizedError",
]


class BaseError(Exception):
    status_code: int


class NotModified(BaseError):
    status_code = 304


class BadRequestError(BaseError):
    status_code = 400


class UnauthorizedError(BaseError):
    status_code = 401


class ForbiddenError(BaseError):
    status_code = 403


class NotFoundError(BaseError):
    status_code = 404


class ConflictError(BaseError):
    status_code = 409


class PreconditionFailedError(BaseError):
    status_code = 412


class NotSupportedError(BaseError):
    status_code = 415


class InternalError(Exception):
    status_code = 500


class LoadError(Exception):
    status_code = 500


class SpecError(Exception):
    status_code = 500
