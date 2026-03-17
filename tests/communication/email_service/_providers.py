from typing import Any

from common.secrets import get_secrets

from x8.communication.email_service import EmailService

secrets = get_secrets()


class EmailServiceProvider:
    SENDGRID = "sendgrid"


provider_parameters: dict[str, dict[str, Any]] = {
    EmailServiceProvider.SENDGRID: {
        "api_key": secrets["SENDGRID_API_KEY"],
    }
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = EmailService(
        __unpack__=True,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
