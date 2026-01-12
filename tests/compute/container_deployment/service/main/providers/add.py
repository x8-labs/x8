import os

import httpx

from x8.core import Provider


class AddProvider(Provider):
    def do(self, a: int) -> int:
        backend_url = os.getenv("BACKEND_URL")
        res = httpx.post(f"{backend_url}/add", json={"a": a, "b": 10})
        return res.json()
