from typing import Any

from x8.core import Provider
from x8.core.exceptions import BadRequestError


class AzureProvider(Provider):
    credential_type: str | None
    tenant_id: str | None
    client_id: str | None
    client_secret: str | None
    certificate_path: str | None

    _credential: Any
    _acredential: Any

    def __init__(
        self,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        **kwargs,
    ):
        self.credential_type = credential_type
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.certificate_path = certificate_path
        self._credential = None
        self._acredential = None
        super().__init__(**kwargs)

    def _get_credential(self):
        if self._credential:
            return self._credential
        credential_type = self.credential_type
        credential = None
        if credential_type == "default":
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()

        elif credential_type == "client_secret":
            from azure.identity import ClientSecretCredential

            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )

        elif credential_type == "certificate":
            from azure.identity import CertificateCredential

            credential = CertificateCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                certificate_path=self.certificate_path,
            )

        elif credential_type == "azure_cli":
            from azure.identity import AzureCliCredential

            credential = AzureCliCredential()

        elif credential_type == "shared_token_cache":
            from azure.identity import SharedTokenCacheCredential

            credential = SharedTokenCacheCredential()

        elif credential_type == "managed_identity":
            from azure.identity import ManagedIdentityCredential

            credential = ManagedIdentityCredential()

        self._credential = credential
        return credential

    def _aget_credential(self):
        if self._acredential:
            return self._acredential
        credential_type = self.credential_type
        credential = None
        if credential_type == "default":
            from azure.identity.aio import DefaultAzureCredential

            credential = DefaultAzureCredential()

        if credential_type == "client_secret":
            from azure.identity.aio import ClientSecretCredential

            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )

        if credential_type == "certificate":
            from azure.identity.aio import CertificateCredential

            credential = CertificateCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                certificate_path=self.certificate_path,
            )

        if credential_type == "azure_cli":
            from azure.identity.aio import AzureCliCredential

            credential = AzureCliCredential()

        if credential_type == "shared_token_cache":
            from azure.identity.aio import SharedTokenCacheCredential

            credential = SharedTokenCacheCredential()

        if credential_type == "managed_identity":
            from azure.identity.aio import ManagedIdentityCredential

            credential = ManagedIdentityCredential()

        self._acredential = credential
        return credential

    def _get_default_subscription_id(self) -> str:
        from azure.mgmt.resource import SubscriptionClient

        sub_client = SubscriptionClient(credential=self._get_credential())
        subs = sub_client.subscriptions.list()
        if not subs:
            raise BadRequestError(
                "No subscriptions found for the given credentials."
            )
        sub = next(sub_client.subscriptions.list())
        return sub.subscription_id

    def _ensure_resource_group(
        self,
        resource_group: str,
        location: str = "westus2",
        subscription_id: str | None = None,
    ) -> None:
        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.resource import ResourceManagementClient

        rg_client = ResourceManagementClient(
            credential=self._get_credential(),
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )
        try:
            rg_client.resource_groups.get(resource_group)
        except ResourceNotFoundError:
            rg_client.resource_groups.create_or_update(
                resource_group, {"location": location}
            )
            print(f"Created resource group: {resource_group}")

    def _delete_resource_group_if_empty(
        self,
        resource_group: str,
        subscription_id: str | None = None,
    ) -> None:
        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.resource import ResourceManagementClient

        rg_client = ResourceManagementClient(
            credential=self._credential,
            subscription_id=subscription_id
            or self._get_default_subscription_id(),
        )

        # Check if the resource group is empty before deleting
        resources = list(
            rg_client.resources.list_by_resource_group(resource_group)
        )
        for resource in resources:
            print(f"Found resource in group: {resource.name}")
        if resources:
            print(
                f"Resource group {resource_group} is not empty; "
                "skipping deletion."
            )
            return
        try:
            operation = rg_client.resource_groups.begin_delete(resource_group)
            operation.result()
            print(f"Deleted resource group: {resource_group}")
        except ResourceNotFoundError:
            print(f"Resource group {resource_group} not found to delete.")
