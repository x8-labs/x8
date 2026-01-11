from __future__ import annotations

__all__ = ["NextJS"]

import asyncio
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from x8.core import Component, Context, Operation, Provider
from x8.interface.api import API


class NextJS(Provider):
    path: str | None
    component: Component | None
    api_host: str | None = None
    api_port: int | None = None
    api_root_path: str | None = None
    api_component: Component | None
    api_url: str | None = None

    _api: API | None

    _template_path = "../templates/nextjs-latest"
    _processes: list[subprocess.Popen]

    def __init__(
        self,
        path: str | None = None,
        component: Component | None = None,
        api_component: Component | None = None,
        api_url: str | None = None,
        api_host: str | None = None,
        api_port: int | None = None,
        api_root_path: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            path:
                Path to the web web.
            component:
                Component to run as a web app.
            api_component:
                API component if API component is separately defined.
            api_url:
                API URL if API is already deployed.
            api_host:
                API host.
            api_port:
                API port.
            api_root_path:
                API root path.
        """
        self.path = path
        self.component = component
        self.api_component = api_component
        self.api_url = api_url
        self.api_host = api_host
        self.api_port = api_port
        self.api_root_path = api_root_path

        self._api = None
        self._processes = []

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        server_thread = threading.Thread(target=self._run_api, daemon=True)
        server_thread.start()
        self._put_env_file()
        try:
            self._run_frontend()
        except KeyboardInterrupt:
            sys.exit(1)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self._put_env_file()
        try:
            await asyncio.gather(
                self._arun_api(),
                self._arun_frontend(),
            )
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
        finally:
            sys.exit(1)

    def _run_frontend(self):
        shell = os.name == "nt"
        os.chdir(self._get_frontend_path())
        subprocess.run(["npm", "install"], shell=shell, check=True)
        subprocess.run(["npm", "run", "dev"], shell=shell, check=True)

    async def _arun_frontend(self):
        shell = os.name == "nt"
        os.chdir(self._get_frontend_path())

        if shell:
            await asyncio.create_subprocess_shell("npm install")
            await asyncio.create_subprocess_shell("npm run dev")
        else:
            await asyncio.create_subprocess_exec("npm", "install")
            await asyncio.create_subprocess_exec("npm", "run", "dev")

    def _run_api(self):
        api = self._get_api()
        if api:
            api.__run__()

    async def _arun_api(self):
        api = self._get_api()
        if api:
            await api.__arun__()

    def _arun_api_in_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._arun_api())
        finally:
            loop.close()

    def _get_api(self) -> API | None:
        component = self.component or self.__component__.component
        if self._api:
            return self._api
        api_component: Any = None
        if self.api_component is not None:
            api_component = self.api_component
        elif component is not None:
            from x8.interface.api import API
            from x8.interface.api.providers.default import Default

            api_component = API(component=component)
            api_component.bind(
                Default(
                    host=self.api_host,
                    port=self.api_port,
                    root_path=self.api_root_path,
                )
            )
        self._api = api_component
        return self._api

    def _get_frontend_path(self) -> str:
        if self.path:
            return self.path
        else:
            return os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    self._template_path,
                )
            )

    def _put_env_file(self) -> None:
        env_file_path = Path(self._get_frontend_path(), ".env")
        api = self._get_api()
        api_url = "http://127.0.0.1:8080"
        if api:
            api_info = api.get_info().result
            if api_info.port:
                api_url = f"{api_info.host}:{api_info.port}"
            else:
                api_url = f"{api_info.host}"
            if api_info.ssl:
                api_url = f"https://{api_url}"
            else:
                api_url = f"http://{api_url}"
        with env_file_path.open("w") as env_file:
            env_file.write(f"API_URL={api_url}\n")
