__all__ = ["Auth0"]

import httpx
import jwt
from jwt import PyJWKClient
from x8.core import Provider
from x8.core.exceptions import ForbiddenError, UnauthorizedError

from .._models import AuthResult, UserCredential, UserInfo


class Auth0(Provider):
    domain: str | None
    client_id: str | None
    client_secret: str | None
    issuer: str | None
    audience: str | None
    scope: str | None
    algorithms: str | list[str] | None
    secret: str | None
    nparams: dict[str, str]

    _jwks_client: PyJWKClient | None
    _jwks_cached_keys: dict

    def __init__(
        self,
        domain: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        issuer: str | None = None,
        audience: str | None = None,
        scope: str | None = None,
        algorithms: str | list[str] | None = "RS256",
        secret: str | None = None,
        nparams: dict[str, str] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            domain:
                Auth0 domain.
            issuer:
                Auth0 issuer.
            client_id:
                Auth0 client id.
            client_secret:
                Auth0 client secret.
            audience:
                Auth0 audience.
            scope:
                Auth0 scope.
            algorithms:
                List of algorithms to use for JWT validation.
            secret:
                Secret key for signing tokens.
            nparams:
                Additional parameters for Auth0.
        """
        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.issuer = issuer
        self.audience = audience
        self.scope = scope
        self.algorithms = algorithms
        self.secret = secret
        self.nparams = nparams
        self._jwks_client = None
        super().__init__(**kwargs)

    def validate(
        self,
        credential: str | UserCredential,
    ) -> AuthResult:
        if not isinstance(credential, str):
            raise ForbiddenError("Token must be a string")
        if self._jwks_client is None:
            self._jwks_client = PyJWKClient(
                f"https://{self.domain}/.well-known/jwks.json"
            )
            self._jwks_cached_keys = {
                jwk.key_id: jwk.key
                for jwk in self._jwks_client.get_signing_keys()
            }
        if credential is None:
            raise ForbiddenError("Token is required for validation")
        try:
            kid = jwt.get_unverified_header(credential).get("kid")
            if kid in self._jwks_cached_keys:
                signing_key = self._jwks_cached_keys[kid]
            else:
                self._cached_keys = {
                    jwk.key_id: jwk.key
                    for jwk in self._jwks_client.get_signing_keys()
                }
                signing_key = self._cached_keys.get(kid)
            if signing_key is None:
                raise ForbiddenError("No valid signing key found")
            payload = jwt.decode(
                credential,
                signing_key,
                algorithms=(
                    self.algorithms
                    if isinstance(self.algorithms, list)
                    else [self.algorithms or "RS256"]
                ),
                audience=self.audience or f"https://{self.domain}/api/v2/",
                issuer=self.issuer or f"https://{self.domain}/",
            )
        except jwt.exceptions.PyJWKClientError as error:
            raise UnauthorizedError(str(error))
        except jwt.exceptions.DecodeError as error:
            raise UnauthorizedError(str(error))
        except Exception as error:
            raise UnauthorizedError(str(error))

        return AuthResult(
            id=payload["sub"],
            email=payload.get("email", None),
            token=credential,
            info=payload,
        )

    def get_user_info(
        self,
        credential: str | UserCredential,
    ) -> UserInfo:
        if not isinstance(credential, str):
            raise ForbiddenError("Token must be a string")
        url = f"https://{self.domain}/userinfo"
        headers = {"Authorization": f"Bearer {credential}"}
        response = httpx.get(url, headers=headers)
        if response.status_code != 200:
            print(
                f"Failed to fetch user info: {response.status_code} "
                f"{response.text}"
            )
            raise UnauthorizedError("Failed to fetch user info")
        payload = response.json()
        user_info = UserInfo(
            id=payload["sub"],
            name=payload.get("name", None),
            email=payload.get("email", None),
            email_verified=payload.get("email_verified", None),
            info=payload,
        )
        if not user_info.id:
            raise UnauthorizedError("User ID is required in user info")
        return user_info
