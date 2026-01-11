from x8.core import Component, operation

from ._models import AuthResult, UserCredential, UserInfo


class Authentication(Component):
    @operation()
    def signup(self, credential: UserCredential) -> AuthResult:
        """Create a new user.

        Args:
            credential: User credential.

        Returns:
            Auth result.
        """
        ...

    @operation()
    def login(self, credential: UserCredential) -> AuthResult:
        """Login user.

        Args:
            credential: User credential.

        Returns:
            Auth result.
        """
        ...

    @operation()
    def refresh(self, credential: str) -> AuthResult:
        """Refresh authentication.

        Args:
            credential: Refresh token or similar credential.

        Returns:
            Refreshed auth result.
        """
        ...

    @operation()
    def validate(self, credential: str | UserCredential) -> AuthResult:
        """Validate credential.

        Args:
            credential: Access token or structured user credential.

        Returns:
            Auth result.
        """
        ...

    @operation()
    def logout(self, credential: str) -> bool:
        """Logout user.

        Args:
            credential: Access token or similar credential.

        Returns:
            True if the user was successfully logged out.
        """
        ...

    @operation()
    def get_user_info(self, credential: str | UserCredential) -> UserInfo:
        """Get user info.

        Args:
            credential: Access token or similar credential.

        Returns:
            Information about the authenticated user.
        """
        ...
