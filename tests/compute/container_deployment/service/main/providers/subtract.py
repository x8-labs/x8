import os

import httpx

from x8.core import Provider


class SubtractProvider(Provider):
    def do(self, a: int) -> int:
        backend_url = os.getenv("BACKEND_URL")
        res = httpx.post(f"{backend_url}/subtract", json={"a": a, "b": 10})
        return res.json()
