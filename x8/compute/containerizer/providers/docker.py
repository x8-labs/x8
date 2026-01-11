"""
Docker provider for containerizer.
"""

__all__ = ["Docker"]

import os
import shutil
import subprocess
import sys
import tempfile

import docker

from x8.core import Provider, Response, RunContext
from x8.core.constants import ROOT_PACKAGE_NAME

from .._helper import create_requirements_file
from .._models import (
    BuildConfig,
    ContainerItem,
    ImageItem,
    PrepareConfig,
    RunConfig,
    SourceItem,
)


class Docker(Provider):
    def __init__(
        self,
        **kwargs,
    ):
        """Initialize."""
        super().__init__(**kwargs)

    def prepare(
        self,
        handle: str,
        config: PrepareConfig = PrepareConfig(),
        run_context: RunContext = RunContext(),
        **kwargs,
    ) -> Response[SourceItem]:
        working_dir = run_context.path
        if not config.prepare_in_place:
            temp_dir = tempfile.mkdtemp()
            shutil.copytree(run_context.path, temp_dir, dirs_exist_ok=True)
            working_dir = temp_dir

        requirements_file = f"requirements-{handle}.txt"
        create_requirements_file(
            handle,
            run_context.tag,
            requirements_file,
            working_dir,
            run_context.manifest,
        )
        requirements_files: list = [requirements_file]
        if isinstance(config.requirements, str):
            requirements_files.append(config.requirements)
        elif isinstance(config.requirements, list):
            requirements_files.extend(config.requirements)
        cmd = [ROOT_PACKAGE_NAME, "run", handle]
        if run_context.tag:
            cmd.append(run_context.tag)
        self._create_dockerfile(
            base_image=config.base_image,
            requirements_files=requirements_files,
            expose=config.expose,
            cmd=cmd,
            out_dir=working_dir,
        )
        result = SourceItem(source=working_dir)
        return Response(result=result)

    def build(
        self,
        source: str,
        config: BuildConfig = BuildConfig(),
        **kwargs,
    ) -> Response[ImageItem]:
        image_name = config.image_name or source.split("/")[-1].split(".")[0]
        if sys.platform == "darwin":
            # docker-py does not support platform parameter correctly
            # on MacOS, so we use subprocess to call docker directly
            cmd: list[str] = [
                "docker",
                "build",
                "--platform",
                config.platform,
                "--rm",
                "--load",
                "-t",
                image_name,
            ]
            if config.nocache:
                cmd += ["--no-cache"]
            cmd += [source]
            subprocess.run(
                cmd,
                check=True,
            )
            client = docker.from_env()
            image = client.images.get(image_name)
            result = self._get_image_item(image)
            return Response(result=result)
        else:
            client = docker.from_env()
            image, build_logs = client.images.build(
                path=source,
                tag=config.image_name,
                nocache=config.nocache,
                rm=True,
                platform=config.platform,
                **kwargs,
            )
            result = self._get_image_item(image)
            return Response(result=result)

    def run(
        self,
        image_name: str,
        config: RunConfig = RunConfig(),
        **kwargs,
    ) -> Response[ContainerItem]:
        client = docker.from_env()
        args = dict(
            image=image_name,
            detach=config.detach,
            remove=config.remove,
        )
        if config.ports is not None:
            args["ports"] = {
                f"{key}/tcp" if not key.endswith("/tcp") else key: value
                for key, value in config.ports.items()
            }
        if config.env is not None:
            args["environment"] = config.env
        container = client.containers.run(**args)
        result = ContainerItem(
            id=container.id,
            name=container.name,
            image=self._get_image_item(container.image),
        )
        return Response(result=result)

    def stop(
        self,
        container_id: str,
        **kwargs,
    ) -> Response[None]:
        client = docker.from_env()
        container = client.containers.get(container_id)
        container.stop()
        return Response(result=None)

    def remove(
        self,
        container_id: str,
        **kwargs,
    ) -> Response[None]:
        client = docker.from_env()
        container = client.containers.get(container_id)
        container.remove(force=True)
        return Response(result=None)

    def delete(
        self,
        image_name: str,
        digest: str | None = None,
        **kwargs,
    ) -> Response[None]:
        client = docker.from_env()
        if digest is None:
            image = client.images.get(image_name)
            client.images.remove(image.id, force=True)
        else:
            image = client.images.get(f"{image_name}@{digest}")
            client.images.remove(image.id, force=True)
        return Response(result=None)

    def tag(
        self,
        image_name: str,
        repository_name: str,
        tag: str,
        digest: str | None = None,
    ) -> Response[None]:
        client = docker.from_env()
        if digest is None:
            image = client.images.get(image_name)
            image.tag(repository_name, tag=tag)
        else:
            image = client.images.get(f"{image_name}@{digest}")
            image.tag(repository_name, tag=tag)
        return Response(result=None)

    def push(
        self,
        image_name: str,
        repository_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        # TODO: throw exception if there is error in push logs
        client = docker.from_env()
        if digest is None:
            image = client.images.get(image_name)
            if tag is not None:
                image.tag(repository_name, tag=tag)
            for line in client.images.push(
                repository_name, tag=tag, stream=True, decode=True
            ):

                print(line)
        else:
            image = client.images.get(f"{image_name}@{digest}")
            if tag is not None:
                image.tag(repository_name, tag=tag)
            for line in client.images.push(
                repository_name, tag=tag, stream=True, decode=True
            ):

                print(line)
        return Response(result=None)

    def pull(
        self,
        image_name: str,
        tag: str | None = None,
    ) -> Response[None]:
        client = docker.from_env()
        if tag is None:
            client.images.pull(image_name)
        else:
            client.images.pull(f"{image_name}:{tag}")
        return Response(result=None)

    def list_images(self) -> Response[list[ImageItem]]:
        client = docker.from_env()
        images = client.images.list()
        image_items = []
        for image in images:
            if image.tags:
                image_items.append(self._get_image_item(image))
        result = image_items
        return Response(result=result)

    def list_containers(self) -> Response[list[ContainerItem]]:
        client = docker.from_env()
        containers = client.containers.list()
        container_items = []
        for container in containers:
            container_items.append(
                ContainerItem(
                    id=container.id,
                    name=container.name,
                    image=self._get_image_item(
                        container.image if container.image else None,
                    ),
                )
            )
        result = container_items
        return Response(result=result)

    def _get_image_item(self, image) -> ImageItem:
        return ImageItem(
            name=image.tags[0].split(":")[0],
            digest=image.id,
            tags=image.tags,
        )

    def _create_dockerfile(
        self,
        base_image,
        requirements_files: list[str] | None,
        expose: int | list[int] | None,
        cmd: list[str] | None,
        out_dir: str,
    ):
        app_dir = "/app"
        lines = [
            f"FROM {base_image}",
            f"WORKDIR {app_dir}",
        ]
        if requirements_files is not None:
            for requirements_file in requirements_files:
                lines.append(f"COPY {requirements_file} {app_dir}")
                pip_install = "RUN pip install --no-cache-dir --upgrade"
                lines.append(f"{pip_install} -r {requirements_file}")
        lines.append(f"COPY . {app_dir}")
        expose_list = None
        if isinstance(expose, int):
            expose_list = [expose]
        elif isinstance(expose, list):
            expose_list = expose
        if expose_list:
            for port in expose_list:
                lines.append(f"EXPOSE {port}")

        if cmd is not None:
            arr = ", ".join(f'"{part}"' for part in cmd)
            lines.append(f"CMD [{arr}]")

        with open(os.path.join(out_dir, "Dockerfile"), "w") as dockerfile:
            dockerfile.write("\n\n".join(lines))

        return lines
