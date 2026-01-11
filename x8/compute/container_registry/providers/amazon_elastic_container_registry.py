import base64
import os
import subprocess
from typing import Any

import boto3
from botocore.exceptions import ClientError
from x8.compute.containerizer import DEFAULT_PLATFORM
from x8.core import Context, Provider, Response
from x8.core.exceptions import BadRequestError

from .._models import ContainerRegistryItem, ContainerRegistryItemDigest


class AmazonElasticContainerRegistry(Provider):
    region: str
    registry_id: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    profile_name: str | None
    platform: str
    nparams: dict[str, Any]

    _client: Any
    _init: bool = False
    _shell: bool = False
    _account_id: str | None = None

    def __init__(
        self,
        region: str = "us-west-2",
        registry_id: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
        platform: str = DEFAULT_PLATFORM,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            region:
                AWS region where the ECR registry is located.
            registry_id:
                AWS account ID that owns the ECR registry.
                If None, uses the default account.
            aws_access_key_id:
                AWS access key ID for authentication.
            aws_secret_access_key:
                AWS secret access key for authentication.
            aws_session_token:
                AWS session token for temporary credentials.
            profile_name:
                AWS profile name to use for authentication.
            platform:
                Platform to use for the container when pulling.
            nparams:
                Additional parameters for the ECR client.
        """
        self.region = region
        self.registry_id = registry_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.platform = platform
        self.nparams = nparams
        self._init = False
        self._ainit = False
        self._client = None
        self._aclient = None
        self._shell = os.name == "nt"
        super().__init__(**kwargs)

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        session_kwargs = {}
        if self.aws_access_key_id:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = (
                self.aws_secret_access_key
            )
        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token
        if self.profile_name:
            session_kwargs["profile_name"] = self.profile_name

        session = boto3.Session(**session_kwargs)
        self._client = session.client(
            "ecr", region_name=self.region, **self.nparams
        )

        if self.registry_id:
            self._account_id = self.registry_id
        else:
            sts = session.client("sts")
            self._account_id = sts.get_caller_identity()["Account"]
        self._init = True

    def _get_registry_url(self) -> str:
        return f"{self._account_id}.dkr.ecr.{self.region}.amazonaws.com"

    def _get_full_image_path(
        self, image_name: str, tag: str | None = None
    ) -> str:
        image = f"{self._get_registry_url()}/{image_name}"
        if tag:
            image += f":{tag}"
        return image

    def _docker_login(self) -> None:
        """Authenticate Docker with ECR."""

        # Get authorization token
        response = self._client.get_authorization_token(
            registryIds=[self._account_id]
        )

        auth_data = response["authorizationData"][0]
        token = auth_data["authorizationToken"]
        endpoint = auth_data["proxyEndpoint"]

        # Decode the token
        username, password = base64.b64decode(token).decode().split(":", 1)

        # Login to Docker
        subprocess.run(
            [
                "docker",
                "login",
                "--username",
                username,
                "--password-stdin",
                endpoint,
            ],
            input=password,
            text=True,
            shell=self._shell,
            check=True,
        )

    def push(self, image_name: str) -> Response[ContainerRegistryItem]:
        self.__setup__()
        self._docker_login()

        full_path = self._get_full_image_path(image_name)
        subprocess.run(
            ["docker", "tag", image_name, full_path],
            shell=self._shell,
            check=True,
        )
        repo_name = image_name.split(":")[0]
        try:
            self._client.describe_repositories(repositoryNames=[repo_name])
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                self._client.create_repository(repositoryName=repo_name)
            else:
                raise
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
        self._docker_login()

        full_path = self._get_full_image_path(image_name, tag)
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
        self._docker_login()

        if digest is None:
            try:
                images = self._client.describe_images(
                    repositoryName=image_name,
                    imageIds=[{"imageTag": "latest"}],
                )["imageDetails"]
            except Exception:
                images = self._client.describe_images(
                    repositoryName=image_name, maxResults=1
                )["imageDetails"]
            if not images:
                raise BadRequestError(
                    f"No images found for {image_name} with tag 'latest'."
                )
            digest = images[0]["imageDigest"]

        source_path = f"{self._get_full_image_path(image_name)}@{digest}"
        tagged_path = f"{self._get_full_image_path(image_name)}:{tag}"

        subprocess.run(
            ["docker", "pull", "--platform", self.platform, source_path],
            shell=self._shell,
            check=True,
        )
        subprocess.run(
            ["docker", "tag", source_path, tagged_path],
            shell=self._shell,
            check=True,
        )
        subprocess.run(
            ["docker", "push", tagged_path], shell=self._shell, check=True
        )

        result = ContainerRegistryItemDigest(
            image_uri=tagged_path,
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
        self.__setup__()

        if digest is None and tag is None:
            # Delete entire repository
            self._client.delete_repository(
                repositoryName=image_name, force=True
            )
        else:
            # Delete specific image
            image_ids = []
            if digest:
                image_ids.append({"imageDigest": digest})
            if tag:
                image_ids.append({"imageTag": tag})

            self._client.batch_delete_image(
                repositoryName=image_name, imageIds=image_ids
            )
        return Response(result=None)

    def list_images(self) -> Response[list[ContainerRegistryItem]]:
        self.__setup__()

        paginator = self._client.get_paginator("describe_repositories")
        items = []

        for page in paginator.paginate():
            for repo in page["repositories"]:
                repo_name = repo["repositoryName"]
                items.append(
                    ContainerRegistryItem(
                        image_name=repo_name,
                        image_uri=self._get_full_image_path(repo_name),
                    )
                )

        result = items
        return Response(result=result)

    def get_digests(
        self, image_name: str
    ) -> Response[list[ContainerRegistryItemDigest]]:
        self.__setup__()

        paginator = self._client.get_paginator("describe_images")
        digests = []

        for page in paginator.paginate(repositoryName=image_name):
            for image_detail in page["imageDetails"]:
                upload_time = None
                if "imagePushedAt" in image_detail:
                    upload_time = image_detail["imagePushedAt"].timestamp()

                image_uri = (
                    f"{self._get_full_image_path(image_name)}"
                    f"@{image_detail['imageDigest']}"
                )

                digests.append(
                    ContainerRegistryItemDigest(
                        image_uri=image_uri,
                        digest=image_detail["imageDigest"],
                        upload_time=upload_time,
                        image_size_bytes=image_detail.get("imageSizeInBytes"),
                        tags=image_detail.get("imageTags"),
                    )
                )

        result = digests
        return Response(result=result)

    def close(self) -> Response[None]:
        self._init = False
        return Response(result=None)
