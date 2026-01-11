from fastapi import HTTPException, status


class UnauthenticatedException(HTTPException):
    def __init__(self, detail: str = "Requires authentication"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
        )


class UnauthorizedException(HTTPException):
    def __init__(self, detail: str = "Not authorized", **kwargs):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
        )


class NotFoundException(HTTPException):
    def __init__(self, detail: str = "Item not found"):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        )


class ConflictException(HTTPException):
    def __init__(self, detail: str = "Item already exists"):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )
