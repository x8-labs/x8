__all__ = ["AzureKubernetesService", "AzureKubernetesServiceResource"]

from typing import Any

from x8._common.azure_provider import AzureProvider
from x8.compute._common._models import ImageMap
from x8.compute.container_registry.component import ContainerRegistry
from x8.compute.containerizer.component import Containerizer
from x8.core import DataModel, Response
from x8.core.exceptions import BadRequestError, NotFoundError

from .._models import ManifestsType
from ._base import BaseKubernetes


class AzureKubernetesServiceResource(DataModel):
    id: str | None
    name: str | None
    location: str
    kubernetes_version: str | None
    fqdn: str | None
    dns_prefix: str | None


class AzureKubernetesService(AzureProvider, BaseKubernetes):
    resource_group: str | None
    name: str | None

    def __init__(
        self,
        resource_group: str | None = None,
        name: str | None = None,
        kubeconfig: str | dict[str, Any] | None = None,
        context: str | None = None,
        manifests: ManifestsType = None,
        overlays: ManifestsType = None,
        namespace: str | None = None,
        images: list[ImageMap] | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.resource_group = resource_group
        self.name = name
        BaseKubernetes.__init__(
            self,
            kubeconfig=kubeconfig,
            context=context,
            manifests=manifests,
            overlays=overlays,
            namespace=namespace,
            images=images,
            containerizer=containerizer,
            container_registry=container_registry,
            **kwargs,
        )
        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def create_resource(
        self,
        resource_group: str | None = None,
        name: str | None = None,
        location: str = "westus2",
        subscription_id: str | None = None,
        kubernetes_version: str | None = None,
        dns_prefix: str | None = None,
        node_count: int = 1,
        vm_size: str = "Standard_DS2_v2",
        enable_rbac: bool = True,
        network_plugin: str | None = None,
        tags: dict[str, str] | None = None,
        sku_tier: str | None = None,
        enable_managed_identity: bool = True,
        auto_upgrade_channel: str | None = None,
        acr_name: str | None = None,
        acr_resource_group: str | None = None,
    ) -> Response[AzureKubernetesServiceResource]:
        resource_group = resource_group or self.resource_group
        name = name or self.name
        if not name:
            raise BadRequestError("Cluster name is required.")

        if not resource_group:
            raise BadRequestError("Resource group is required.")

        # Ensure RG exists
        self._ensure_resource_group(
            resource_group=resource_group,
            location=location,
            subscription_id=subscription_id,
        )

        from azure.mgmt.containerservice import ContainerServiceClient
        from azure.mgmt.containerservice.models import (
            ContainerServiceNetworkProfile,
            ManagedCluster,
            ManagedClusterAgentPoolProfile,
            ManagedClusterAutoUpgradeProfile,
            ManagedClusterIdentity,
            ManagedClusterSKU,
        )

        client = ContainerServiceClient(
            credential=self._get_credential(),
            subscription_id=(
                subscription_id or self._get_default_subscription_id()
            ),
        )
        agent_pools = [
            ManagedClusterAgentPoolProfile(
                name="nodepool1",
                count=node_count,
                vm_size=vm_size,
                mode="System",
                type="VirtualMachineScaleSets",
                os_type="Linux",
            )
        ]

        sku = None
        if sku_tier:
            sku = ManagedClusterSKU(name="Base", tier=sku_tier)

        net_profile = None
        if network_plugin:
            net_profile = ContainerServiceNetworkProfile(
                network_plugin=network_plugin
            )
        mc = ManagedCluster(
            location=location,
            dns_prefix=dns_prefix or f"{name}-dns",
            kubernetes_version=kubernetes_version,
            enable_rbac=enable_rbac,
            identity=(
                ManagedClusterIdentity(type="SystemAssigned")
                if enable_managed_identity
                else None
            ),
            agent_pool_profiles=agent_pools,
            sku=sku,
            network_profile=net_profile,
            tags=tags,
            auto_upgrade_profile=(
                ManagedClusterAutoUpgradeProfile(
                    upgrade_channel=auto_upgrade_channel
                )
                if auto_upgrade_channel
                else None
            ),
        )

        poller = client.managed_clusters.begin_create_or_update(
            resource_group_name=resource_group,
            resource_name=name,
            parameters=mc,
        )
        result = poller.result()

        # Optionally attach ACR (grant AcrPull to kubelet identity)
        if acr_name:
            try:
                self._attach_acr_to_cluster(
                    subscription_id=str(
                        subscription_id or self._get_default_subscription_id()
                    ),
                    resource_group=resource_group,
                    cluster_name=name,
                    acr_name=acr_name,
                    acr_resource_group=acr_resource_group or resource_group,
                )
            except Exception as e:
                # Surface a concise error but do not fail cluster creation
                print(f"Warning: failed to attach ACR '{acr_name}': {e}")

        resource = AzureKubernetesServiceResource(
            id=result.id,
            name=result.name,
            location=result.location,
            kubernetes_version=result.kubernetes_version,
            fqdn=result.fqdn or result.private_fqdn,
            dns_prefix=result.dns_prefix,
        )
        return Response(result=resource)

    def _attach_acr_to_cluster(
        self,
        subscription_id: str,
        resource_group: str,
        cluster_name: str,
        acr_name: str,
        acr_resource_group: str,
    ) -> None:
        """
        Grant AcrPull role on the ACR to the AKS kubelet identity if available,
        otherwise to the cluster principal. Requires managed identity or SP.
        """
        import uuid

        from azure.mgmt.authorization import AuthorizationManagementClient
        from azure.mgmt.authorization.models import (
            RoleAssignmentCreateParameters,
        )
        from azure.mgmt.containerregistry import (
            ContainerRegistryManagementClient,
        )
        from azure.mgmt.containerservice import ContainerServiceClient

        # Get cluster to obtain identities
        aks_client = ContainerServiceClient(
            credential=self._get_credential(), subscription_id=subscription_id
        )
        mc = aks_client.managed_clusters.get(resource_group, cluster_name)

        # Get ACR resource id
        acr_client = ContainerRegistryManagementClient(
            credential=self._get_credential(), subscription_id=subscription_id
        )
        registry = acr_client.registries.get(
            resource_group_name=acr_resource_group,
            registry_name=acr_name,
        )
        scope = registry.id

        # Determine principal object id to grant
        principal_object_id: str | None = None
        identity_profile = mc.identity_profile
        if identity_profile and isinstance(identity_profile, dict):
            kubelet = identity_profile.get("kubeletidentity")
            if kubelet is not None:
                principal_object_id = kubelet.object_id
                if not principal_object_id:
                    principal_object_id = getattr(
                        kubelet, "principal_id", None
                    )
        if not principal_object_id:
            principal_object_id = (
                mc.identity.principal_id if mc.identity else None
            )
        if not principal_object_id:
            raise RuntimeError(
                "Could not determine AKS principal to assign AcrPull."
            )

        # Assign AcrPull role (built-in role id)
        auth_client = AuthorizationManagementClient(
            credential=self._get_credential(), subscription_id=subscription_id
        )
        role_def_id = (
            f"/subscriptions/{subscription_id}/providers/"
            f"Microsoft.Authorization/roleDefinitions/"
            f"7f951dda-4ed3-4680-a7ca-43fe172d538d"
        )  # AcrPull
        assignment_name = str(uuid.uuid4())
        from azure.mgmt.authorization.models import RoleAssignmentProperties

        params = RoleAssignmentCreateParameters(
            properties=RoleAssignmentProperties(
                role_definition_id=role_def_id,
                principal_id=principal_object_id,
            )
        )
        try:
            auth_client.role_assignments.create(scope, assignment_name, params)
        except Exception as e:
            # Ignore conflicts if role already assigned
            msg = str(e).lower()
            if "conflict" in msg or "already exists" in msg:
                return
            raise

    def get_resource(
        self,
        resource_group: str,
        name: str,
        subscription_id: str | None = None,
    ) -> Response[AzureKubernetesServiceResource]:
        if not name:
            raise BadRequestError("Cluster name is required.")
        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.containerservice import ContainerServiceClient

        client = ContainerServiceClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        try:
            result = client.managed_clusters.get(resource_group, name)
            resource = AzureKubernetesServiceResource(
                id=result.id,
                name=result.name,
                location=result.location,
                kubernetes_version=result.kubernetes_version,
                fqdn=result.fqdn or result.private_fqdn,
                dns_prefix=result.dns_prefix,
            )
        except ResourceNotFoundError:
            raise NotFoundError("Kubernetes cluster not found.")
        return Response(result=resource)

    def delete_resource(
        self,
        resource_group: str,
        name: str,
        subscription_id: str | None = None,
        delete_empty_resource_group: bool = False,
    ) -> Response[None]:
        if not name:
            raise BadRequestError("Cluster name is required.")

        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.containerservice import ContainerServiceClient

        client = ContainerServiceClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        try:
            poller = client.managed_clusters.begin_delete(resource_group, name)
            poller.wait()
        except ResourceNotFoundError:
            raise NotFoundError("Kubernetes cluster not found.")

        if delete_empty_resource_group:
            self._delete_resource_group_if_empty(
                resource_group=resource_group,
                subscription_id=subscription_id,
            )
        return Response(result=None)

    def _get_provider_kubeconfig(self) -> str | dict[str, Any] | None:
        import base64

        import yaml
        from azure.mgmt.containerservice import ContainerServiceClient

        if self.resource_group is None:
            raise BadRequestError("Resource group is required.")
        if self.name is None:
            raise BadRequestError("Cluster name is required.")

        client = ContainerServiceClient(
            credential=self._get_credential(),
            subscription_id=self._get_default_subscription_id(),
        )
        k = client.managed_clusters.list_cluster_user_credentials(
            resource_group_name=self.resource_group, resource_name=self.name
        )
        if not k.kubeconfigs or len(k.kubeconfigs) == 0:
            raise NotFoundError("No kubeconfig found for the cluster.")
        kubeconfig_b64 = k.kubeconfigs[0].value  # bytes (base64)
        if kubeconfig_b64 is None:
            raise NotFoundError("No kubeconfig value found for the cluster.")
        kubeconfig_yaml = base64.b64decode(kubeconfig_b64).decode("utf-8")
        kubeconfig_obj = yaml.safe_load(kubeconfig_yaml)

        return kubeconfig_obj
