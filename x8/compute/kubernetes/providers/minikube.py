import shutil
import subprocess
from typing import Any

from ._base import BaseKubernetes


class MinikubeKubernetes(BaseKubernetes):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _minikube_available(self) -> bool:
        return shutil.which("minikube") is not None

    def _load_images(self, images: list[str]) -> None:
        """
        Idempotently load images into minikube.
        Safe to call multiple times; minikube will skip already-present images.
        """
        if not images:
            return
        if not self._minikube_available():
            # Minikube CLI not present -> nothing to do
            print("Minikube CLI not found")
            return
        for img in images:
            # `--overwrite=true` ensures latest local layers are taken
            # `--pull=false` avoids network pulls for local-only tags
            cmd = [
                "minikube",
                "image",
                "load",
                img,
                "--overwrite=true",
                "--pull=false",
            ]
            try:
                subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                print(f"Loaded image {img} to minikube")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    (
                        f"minikube image load failed for {img}:\n"
                        f"{e.stdout.decode('utf-8', errors='ignore')}"
                    )
                )

    def _delete_images(self, images: list[str]) -> None:
        if not images:
            return
        if not self._minikube_available():
            print("Minikube CLI not found")
            return
        for img in images:
            cmd = [
                "minikube",
                "image",
                "rm",
                img,
            ]
            try:
                subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                print(f"Deleted image {img} from minikube")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    (
                        f"minikube image delete failed for {img}:\n"
                        f"{e.stdout.decode('utf-8', errors='ignore')}"
                    )
                )

    def _apply_provider_settings(
        self,
        objects: list[dict[str, Any]],
        image_map: dict[str, str],
    ):
        self._load_images(list(image_map.values()))
