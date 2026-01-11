import os
import subprocess
from typing import Any, Literal

from x8._common.azure_provider import AzureProvider
from x8.compute.containerizer import DEFAULT_PLATFORM
from x8.core import Context, DataModel, Response
from x8.core.exceptions import BadRequestError, NotFoundError

from .._models import ContainerRegistryItem, ContainerRegistryItemDigest


class AzureContainerRegistryResource(DataModel):
    id: str
    name: str
    login_server: str
    location: str


class AzureContainerRegistry(AzureProvider):
    name: str | None
    platform: str
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _init: bool = False
    _ainit: bool = False
    _shell: bool = False

    def __init__(
        self,
        name: str | None = None,
        platform: str = DEFAULT_PLATFORM,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            name:
                Azure Container Registry name (without .azurecr.io suffix).
            credential_type:
                Type of Azure credential to use. Options are:
                - default: DefaultAzureCredential
                - client_secret: ClientSecretCredential
                - certificate: CertificateCredential
                - azure_cli: AzureCliCredential
                - shared_token_cache: SharedTokenCacheCredential
                - managed_identity: ManagedIdentityCredential
            tenant_id:
                Tenant ID for the Azure account.
            client_id:
                Client ID for the Azure account.
            client_secret:
                Client secret for the Azure account.
            certificate_path:
                Path to the certificate for the Azure account.
            platform:
                Platform to use for the container when pulling.
            nparams:
                Additional parameters for the Azure Container Registry client.
        """
        self.name = name
        self.platform = platform
        self.nparams = nparams
        self._init = False
        self._ainit = False
        self._client = None
        self._aclient = None
        self._shell = os.name == "nt"
        super().__init__(
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        from azure.containerregistry import ContainerRegistryClient

        self._credential = self._get_credential()
        endpoint = f"https://{self._get_name()}.azurecr.io"

        self._client = ContainerRegistryClient(
            endpoint=endpoint,
            credential=self._credential,
            **self.nparams,
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        from azure.containerregistry.aio import ContainerRegistryClient

        self._acredential = self._aget_credential()
        endpoint = f"https://{self._get_name()}.azurecr.io"

        self._aclient = ContainerRegistryClient(
            endpoint=endpoint,
            credential=self._acredential,
            **self.nparams,
        )
        self._ainit = True

    def _get_registry_url(self) -> str:
        return f"{self._get_name()}.azurecr.io"

    def _get_full_image_path(
        self, image_name: str, tag: str | None = None
    ) -> str:
        image = f"{self._get_registry_url()}/{image_name}"
        if tag:
            image += f":{tag}"
        return image

    def create_resource(
        self,
        resource_group: str,
        name: str | None = None,
        location: str = "westus2",
        subscription_id: str | None = None,
        sku: Literal["Basic", "Standard", "Premium"] = "Standard",
        admin_user_enabled: bool = True,
        public_network_access: Literal["Enabled", "Disabled"] = "Enabled",
        data_endpoint_enabled: bool = False,
        network_rule_bypass_options: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> Response[AzureContainerRegistryResource]:
        registry_name = name or self.name

        if not registry_name:
            raise BadRequestError("Registry name is required.")
        self._ensure_resource_group(
            resource_group=resource_group,
            location=location,
            subscription_id=subscription_id,
        )

        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.containerregistry import (
            ContainerRegistryManagementClient,
        )
        from azure.mgmt.containerregistry.models import (
            Registry,
            RegistryUpdateParameters,
            Sku,
        )

        client = ContainerRegistryManagementClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        create_params = Registry(
            location=location,
            sku=Sku(name=sku),
            admin_user_enabled=admin_user_enabled,
            public_network_access=public_network_access,
            data_endpoint_enabled=data_endpoint_enabled,
            network_rule_bypass_options=network_rule_bypass_options,
            tags=tags,
        )

        try:
            existing = client.registries.get(
                resource_group_name=resource_group,
                registry_name=registry_name,
            )
            if (existing.location or "").lower() != (location or "").lower():
                raise BadRequestError(
                    (
                        f"Registry '{registry_name}' already in location "
                        f"'{existing.location}'."
                    )
                )
            update_params = RegistryUpdateParameters(
                sku=Sku(name=sku),
                admin_user_enabled=admin_user_enabled,
                public_network_access=public_network_access,
                data_endpoint_enabled=data_endpoint_enabled,
                network_rule_bypass_options=network_rule_bypass_options,
                tags=tags,
            )
            poller = client.registries.begin_update(
                resource_group_name=resource_group,
                registry_name=registry_name,
                registry_update_parameters=update_params,
            )
            result = poller.result()
        except ResourceNotFoundError:
            poller = client.registries.begin_create(
                resource_group_name=resource_group,
                registry_name=registry_name,
                registry=create_params,
            )
            result = poller.result()

        resource = AzureContainerRegistryResource(
            id=result.id,
            name=result.name,
            login_server=result.login_server,
            location=result.location,
        )
        return Response(result=resource)

    def get_resource(
        self,
        resource_group: str,
        name: str | None = None,
        subscription_id: str | None = None,
    ) -> Response[AzureContainerRegistryResource]:
        registry_name = name or self.name
        if not registry_name:
            raise BadRequestError("Registry name is required.")

        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.containerregistry import (
            ContainerRegistryManagementClient,
        )

        client = ContainerRegistryManagementClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        try:
            registry = client.registries.get(
                resource_group_name=resource_group,
                registry_name=registry_name,
            )
            result = AzureContainerRegistryResource(
                id=registry.id,
                name=registry.name,
                location=registry.location,
                login_server=registry.login_server,
            )
        except ResourceNotFoundError:
            raise NotFoundError("Container registry not found.")
        return Response(result=result)

    def delete_resource(
        self,
        resource_group: str,
        name: str | None = None,
        subscription_id: str | None = None,
        delete_empty_resource_group: bool = False,
    ) -> Response[None]:
        registry_name = name or self.name
        if not registry_name:
            raise BadRequestError("Registry name is required.")

        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.containerregistry import (
            ContainerRegistryManagementClient,
        )

        client = ContainerRegistryManagementClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        try:
            poller = client.registries.begin_delete(
                resource_group_name=resource_group,
                registry_name=registry_name,
            )
            poller.wait()
        except ResourceNotFoundError:
            raise NotFoundError("Container registry not found.")
        if delete_empty_resource_group:
            self._delete_resource_group_if_empty(
                resource_group=resource_group,
                subscription_id=subscription_id,
            )
        return Response(result=None)

    def push(self, image_name: str) -> Response[ContainerRegistryItem]:
        subprocess.run(
            ["az", "acr", "login", "--name", self._get_name()],
            shell=self._shell,
            check=True,
        )

        full_path = self._get_full_image_path(image_name)
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
        subprocess.run(
            ["az", "acr", "login", "--name", self._get_name()],
            shell=self._shell,
            check=True,
        )

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
        if digest is None:
            latest_tag_props = self._client.get_manifest_properties(
                image_name, "latest"
            )
            digest = latest_tag_props.digest
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
            self._client.delete_repository(image_name)
        elif digest is not None:
            self._client.delete_manifest(
                repository=image_name,
                tag_or_digest=digest,
            )
        elif tag is not None:
            self._client.delete_manifest(
                repository=image_name,
                tag_or_digest=tag,
            )
        return Response(result=None)

    def list_images(self) -> Response[list[ContainerRegistryItem]]:
        self.__setup__()
        repositories = self._client.list_repository_names()
        items = []

        for repo_name in repositories:
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
        manifests = self._client.list_manifest_properties(image_name)
        digests = []
        for manifest in manifests:
            upload_time = None
            if manifest.created_on:
                upload_time = manifest.created_on.timestamp()
            image_uri = (
                f"{self._get_full_image_path(image_name)}@{manifest.digest}"
            )
            digests.append(
                ContainerRegistryItemDigest(
                    image_uri=image_uri,
                    digest=manifest.digest,
                    upload_time=upload_time,
                    image_size_bytes=manifest.size_in_bytes,
                    tags=list(manifest.tags) if manifest.tags else None,
                )
            )

        result = digests
        return Response(result=result)

    async def adelete(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        await self.__asetup__()
        if digest is None and tag is None:
            await self._aclient.delete_repository(image_name)
        elif digest is not None:
            await self._aclient.delete_manifest(
                repository=image_name,
                tag_or_digest=digest,
            )
        elif tag is not None:
            await self._aclient.delete_manifest(
                repository=image_name,
                tag_or_digest=tag,
            )
        return Response(result=None)

    async def alist_images(self) -> Response[list[ContainerRegistryItem]]:
        await self.__asetup__()
        repositories = self._aclient.list_repository_names()
        items = []
        async for repo_name in repositories:
            items.append(
                ContainerRegistryItem(
                    image_name=repo_name,
                    image_uri=f"{self._get_registry_url()}/{repo_name}",
                )
            )

        result = items
        return Response(result=result)

    async def aget_digests(
        self, image_name: str
    ) -> Response[list[ContainerRegistryItemDigest]]:
        await self.__asetup__()
        manifests = self._aclient.list_manifest_properties(image_name)
        digests = []
        async for manifest in manifests:
            upload_time = None
            if manifest.created_on:
                upload_time = manifest.created_on.timestamp()
            image_uri = (
                f"{self._get_full_image_path(image_name)}@{manifest.digest}"
            )
            digests.append(
                ContainerRegistryItemDigest(
                    image_uri=image_uri,
                    digest=manifest.digest,
                    upload_time=upload_time,
                    image_size_bytes=manifest.size_in_bytes,
                    tags=list(manifest.tags) if manifest.tags else None,
                )
            )
        result = digests
        return Response(result=result)

    def close(self) -> Response[None]:
        self._init = False
        return Response(result=None)

    async def aclose(self) -> Response[None]:
        if self._acredential:
            await self._acredential.close()
        if self._aclient:
            await self._aclient.close()
            # Close doesn't cleanup properly in the current client
            # Might be a bug in the client, so we need the following
            await self._aclient.__aexit__(None, None, None)
        self._ainit = False
        return Response(result=None)

    def _get_name(self) -> str:
        if not self.name:
            raise BadRequestError("Registry name is required.")
        return self.name
