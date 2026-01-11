import json
from typing import Any

from x8.core import Provider


class GoogleProvider(Provider):
    service_account_info: str | None
    service_account_file: str | None
    access_token: str | None

    _credentials: Any

    def __init__(
        self,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        **kwargs,
    ):
        self.service_account_info = service_account_info
        self.service_account_file = service_account_file
        self.access_token = access_token
        self._credentials = None
        super().__init__(**kwargs)

    def _get_credentials(self):
        if self._credentials:
            return self._credentials
        from google.oauth2 import service_account

        if self.service_account_info is not None:
            service_account_info = (
                json.loads(self.service_account_info)
                if isinstance(self.service_account_info, str)
                else self.service_account_info
            )
            self._credentials = (
                service_account.Credentials.from_service_account_info(
                    service_account_info
                )
            )
        elif self.service_account_file is not None:
            self._credentials = (
                service_account.Credentials.from_service_account_file(
                    self.service_account_file
                )
            )
        elif self.access_token is not None:
            from google.auth.credentials import Credentials

            self._credentials = Credentials(self.access_token)
        return self._credentials

    def _get_project_or_default(self, project: str | None) -> str:
        if project:
            return project
        return self._get_default_project()

    def _get_default_project(self) -> str:
        import google.auth

        _, project = google.auth.default()
        return project
