import os
import subprocess
from typing import Any, Literal

from x8._common.google_provider import GoogleProvider
from x8.compute.containerizer import DEFAULT_PLATFORM
from x8.core import Context, DataModel, Response
from x8.core.exceptions import BadRequestError, NotFoundError

from .._models import ContainerRegistryItem, ContainerRegistryItemDigest


class GoogleArtifactRegistryResource(DataModel):
    id: str
    name: str
    location: str
    login_server: str


class GoogleArtifactRegistry(GoogleProvider):
    project: str | None
    location: str | None
    name: str | None
    platform: str
    nparams: dict[str, Any] | None

    _client: Any
    _aclient: Any
    _credentials: Any
    _shell: bool = False
    _init: bool = False
    _ainit: bool = False

    def __init__(
        self,
        project: str | None = None,
        location: str = "us-central1",
        name: str | None = None,
        platform: str = DEFAULT_PLATFORM,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            project:
                Google Cloud project ID.
            location:
                Google Cloud location.
            name:
                Google Artifact Registry repository name.
            platform:
                Platform to use for the container when pulling.
            service_account_info:
                Google service account info with serialized credentials.
            service_account_file:
                Google service account file with credentials.
            access_token:
                Google access token.
            nparams:
                Native params to google client.
        """
        self.project = project
        self.location = location
        self.name = name
        self.platform = platform
        self.nparams = nparams
        self._init = False
        self._ainit = False
        self._shell = os.name == "nt"
        super().__init__(
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        from google.cloud import artifactregistry_v1

        self._client = artifactregistry_v1.ArtifactRegistryClient(
            credentials=self._get_credentials(),
            **(self.nparams or {}),
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        from google.cloud import artifactregistry_v1

        self._aclient = artifactregistry_v1.ArtifactRegistryAsyncClient(
            credentials=self._get_credentials(),
            **(self.nparams or {}),
        )
        self._ainit = True

    def _artifact_registry_path(self) -> str:
        return f"{self.location}-docker.pkg.dev"

    def _repository_path(self) -> str:
        return (
            f"projects/{self._get_project_or_default(self.project)}/"
            f"locations/{self.location}/"
            f"repositories/{self.name}"
        )

    def _full_image_path(self, image_name: str, tag: str | None = None) -> str:
        if self.name:
            image = (
                f"{self._artifact_registry_path()}/"
                f"{self._get_project_or_default(self.project)}/"
                f"{self.name}/{image_name}"
            )
        else:
            image = (
                f"gcr.io/{self._get_project_or_default(self.project)}/"
                f"{image_name}"
            )
        if tag:
            image += f":{tag}"
        return image

    def create_resource(
        self,
        project: str | None = None,
        name: str | None = None,
        location: str | None = None,
        description: str | None = None,
        labels: dict[str, str] | None = None,
        format: Literal[
            "DOCKER", "MAVEN", "NPM", "PYTHON", "APT", "YUM"
        ] = "DOCKER",
        mode: Literal["STANDARD", "VIRTUAL", "REMOTE"] = "STANDARD",
    ) -> Response[GoogleArtifactRegistryResource]:
        from google.api_core.exceptions import NotFound
        from google.cloud import artifactregistry_v1
        from google.cloud.artifactregistry_v1 import Repository
        from google.protobuf.field_mask_pb2 import FieldMask

        repo_id = name or self.name
        if not repo_id:
            raise BadRequestError("Repository name is required.")

        location_input = location or self.location
        if not location_input:
            raise BadRequestError("Location is required.")

        project = project or self._get_project_or_default(project=self.project)
        client = artifactregistry_v1.ArtifactRegistryClient(
            credentials=self._get_credentials()
        )

        parent = f"projects/{project}/locations/{location_input}"
        repo_name = f"{parent}/repositories/{repo_id}"

        # Map format/mode to enums
        fmt_map = {
            "DOCKER": Repository.Format.DOCKER,
            "MAVEN": Repository.Format.MAVEN,
            "NPM": Repository.Format.NPM,
            "PYTHON": Repository.Format.PYTHON,
            "APT": Repository.Format.APT,
            "YUM": Repository.Format.YUM,
        }
        mode_map = {
            "STANDARD": Repository.Mode.STANDARD_REPOSITORY,
            "VIRTUAL": Repository.Mode.VIRTUAL_REPOSITORY,
            "REMOTE": Repository.Mode.REMOTE_REPOSITORY,
        }

        try:
            existing = client.get_repository(name=repo_name)
            update = Repository(
                name=repo_name,
                description=(
                    description
                    if description is not None
                    else existing.description
                ),
                labels=labels if labels is not None else dict(existing.labels),
            )
            mask_paths: list[str] = []
            if description is not None:
                mask_paths.append("description")
            if labels is not None:
                mask_paths.append("labels")

            if mask_paths:
                result = client.update_repository(
                    repository=update,
                    update_mask=FieldMask(paths=mask_paths),
                )
            else:
                result = existing
        except NotFound:
            create = Repository(
                format=fmt_map.get(format, Repository.Format.DOCKER),
                mode=mode_map.get(mode, Repository.Mode.STANDARD_REPOSITORY),
                description=description or "",
                labels=labels or {},
            )
            op = client.create_repository(
                parent=parent,
                repository_id=repo_id,
                repository=create,
            )
            result = op.result()

        login_server = f"{location_input}-docker.pkg.dev/{project}/{repo_id}"
        resource = GoogleArtifactRegistryResource(
            id=result.name,
            name=repo_id,
            location=location_input,
            login_server=login_server,
        )
        return Response(result=resource)

    def get_resource(
        self,
        project: str | None = None,
        name: str | None = None,
        location: str | None = None,
    ) -> Response[GoogleArtifactRegistryResource]:
        from google.api_core.exceptions import NotFound
        from google.cloud import artifactregistry_v1

        repo_id = name or self.name
        if not repo_id:
            raise BadRequestError("Repository name is required.")

        location_input = location or self.location
        if not location_input:
            raise BadRequestError("Location is required.")

        project = project or self._get_project_or_default(project=self.project)
        client = artifactregistry_v1.ArtifactRegistryClient(
            credentials=self._get_credentials()
        )

        parent = f"projects/{project}/locations/{location_input}"
        repo_name = f"{parent}/repositories/{repo_id}"

        try:
            repo = client.get_repository(name=repo_name)
        except NotFound:
            raise NotFoundError("Artifact Registry repository not found.")

        login_server = f"{location_input}-docker.pkg.dev/{project}/{repo_id}"
        result = GoogleArtifactRegistryResource(
            id=repo.name,
            name=repo_id,
            location=location_input,
            login_server=login_server,
        )
        return Response(result=result)

    def delete_resource(
        self,
        project: str | None = None,
        name: str | None = None,
        location: str | None = None,
    ) -> Response[None]:
        from google.api_core.exceptions import NotFound
        from google.cloud import artifactregistry_v1

        repo_id = name or self.name
        if not repo_id:
            raise BadRequestError("Repository name is required.")

        location_input = location or self.location
        if not location_input:
            raise BadRequestError("Location is required.")

        project = project or self._get_project_or_default(project=self.project)
        client = artifactregistry_v1.ArtifactRegistryClient(
            credentials=self._get_credentials()
        )

        parent = f"projects/{project}/locations/{location_input}"
        repo_name = f"{parent}/repositories/{repo_id}"

        try:
            client.get_repository(name=repo_name)
        except NotFound:
            raise NotFoundError("Artifact Registry repository not found.")

        op = client.delete_repository(name=repo_name)
        op.result()

        return Response(result=None)

    def push(self, image_name: str) -> Response[ContainerRegistryItem]:
        cmd = [
            "gcloud",
            "auth",
            "configure-docker",
        ]
        if self.name:
            cmd.append(self._artifact_registry_path())
        subprocess.run(
            cmd,
            shell=self._shell,
            check=True,
        )
        full_path = self._full_image_path(image_name)
        subprocess.run(
            ["docker", "tag", image_name, full_path],
            shell=self._shell,
            check=True,
        )
        subprocess.run(
            ["docker", "push", full_path], shell=self._shell, check=True
        )
        result = ContainerRegistryItem(
            image_name=image_name, image_uri=full_path
        )
        return Response(result=result)

    def pull(
        self, image_name: str, tag: str | None = None
    ) -> Response[ContainerRegistryItem]:
        full_path = self._full_image_path(image_name, tag)
        subprocess.run(
            [
                "gcloud",
                "auth",
                "configure-docker",
                self._artifact_registry_path(),
            ],
            shell=self._shell,
            check=True,
        )
        subprocess.run(
            ["docker", "pull", "--platform", self.platform, full_path],
            shell=self._shell,
            check=True,
        )
        result = ContainerRegistryItem(
            image_name=image_name, image_uri=full_path
        )
        return Response(result=result)

    def tag(
        self,
        image_name: str,
        tag: str,
        digest: str | None = None,
    ) -> Response[ContainerRegistryItemDigest]:
        self.__setup__()
        image_uri_base = self._full_image_path(image_name)
        if digest is None:
            digests = self.get_digests(image_name).result
            if not digests:
                raise BadRequestError(
                    f"No digests found for image '{image_name}'"
                )
            digest_entry = max(digests, key=lambda d: d.upload_time or 0)
            digest = digest_entry.digest

        subprocess.run(
            [
                "gcloud",
                "artifacts",
                "docker",
                "tags",
                "add",
                f"{image_uri_base}@{digest}",
                f"{image_uri_base}:{tag}",
            ],
            shell=self._shell,
            check=True,
        )
        result = ContainerRegistryItemDigest(
            image_uri=f"{image_uri_base}:{tag}",
            digest=digest,
            upload_time=None,
            image_size_bytes=None,
            tags=[tag],
        )
        return Response(result=result)

    def delete(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        full_path = self._full_image_path(image_name)

        if tag is not None and digest is None:
            subprocess.run(
                [
                    "gcloud",
                    "artifacts",
                    "docker",
                    "tags",
                    "delete",
                    f"{full_path}:{tag}",
                    "--quiet",
                ],
                shell=self._shell,
                check=True,
            )
            return Response(result=None)

        if digest is not None:
            target = f"{full_path}@{digest}"
        else:
            target = full_path

        subprocess.run(
            [
                "gcloud",
                "artifacts",
                "docker",
                "images",
                "delete",
                target,
                "--delete-tags",
                "--quiet",
            ],
            shell=self._shell,
            check=True,
        )
        return Response(result=None)

    def list_images(self) -> Response[list[ContainerRegistryItem]]:
        self.__setup__()
        parent = self._repository_path()
        results = self._client.list_docker_images(parent=parent)

        seen = set()
        items = []

        for image in results:
            parts = image.uri.split("/")
            image_path = parts[-1]
            base_name, digest = image_path.split("@")

            if base_name not in seen:
                seen.add(base_name)
                items.append(
                    ContainerRegistryItem(
                        image_name=base_name,
                        image_uri=image.uri.split("@")[0],
                    )
                )

        result = items
        return Response(result=result)

    def get_digests(
        self, image_name: str
    ) -> Response[list[ContainerRegistryItemDigest]]:
        self.__setup__()
        parent = self._repository_path()
        results = self._client.list_docker_images(parent=parent)
        digests = []

        for image in results:
            parts = image.name.split("/")
            image_path = parts[-1]
            base_name, digest = image_path.split("@")
            if base_name == image_name:
                digests.append(
                    ContainerRegistryItemDigest(
                        image_uri=image.uri,
                        digest=digest,
                        upload_time=image.upload_time.timestamp(),
                        image_size_bytes=image.image_size_bytes,
                        tags=list(image.tags) if image.tags else None,
                    )
                )

        result = digests
        return Response(result=result)

    async def alist_images(self) -> Response[list[ContainerRegistryItem]]:
        await self.__asetup__()
        parent = self._repository_path()
        results = await self._aclient.list_docker_images(parent=parent)

        seen = set()
        items = []

        async for image in results:
            parts = image.uri.split("/")
            image_path = parts[-1]
            base_name, digest = image_path.split("@")

            if base_name not in seen:
                seen.add(base_name)
                items.append(
                    ContainerRegistryItem(
                        image_name=base_name,
                        image_uri="/".join(parts[:-1]),
                    )
                )

        result = items
        return Response(result=result)

    async def aget_digests(
        self, image_name: str
    ) -> Response[list[ContainerRegistryItemDigest]]:
        await self.__asetup__()
        parent = self._repository_path()
        results = await self._aclient.list_docker_images(parent=parent)
        digests = []

        async for image in results:
            parts = image.name.split("/")
            image_path = parts[-1]
            base_name, digest = image_path.split("@")
            if base_name == image_name:
                digests.append(
                    ContainerRegistryItemDigest(
                        image_uri=image.uri,
                        digest=digest,
                        upload_time=image.upload_time.timestamp(),
                        image_size_bytes=image.image_size_bytes,
                        tags=list(image.tags) if image.tags else None,
                    )
                )

        result = digests
        return Response(result=result)

    def close(self) -> Response[None]:
        self._init = False
        return Response(result=None)

    async def aclose(self) -> Response[None]:
        self._ainit = False
        return Response(result=None)
