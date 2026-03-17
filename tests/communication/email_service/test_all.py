import pytest

from x8.communication.email_service import EmailSendResult
from x8.core.exceptions import BadRequestError

from ._providers import EmailServiceProvider
from ._sync_and_async_client import EmailServiceSyncAndAsyncClient

TEST_FROM = "twoamowl@gmail.com"
TEST_TO = "slenin@gmail.com"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False],
)
async def test_send_simple_text(provider_type: str, async_call: bool):
    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.send(
        to=TEST_TO,
        content="Hello from x8 test",
        from_email=TEST_FROM,
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.status is not None
    assert result.channel == "email"
    assert result.provider == "sendgrid"
    assert result.from_ == TEST_FROM
    assert TEST_TO in result.to


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_html(provider_type: str, async_call: bool):
    from x8.communication.email_service import EmailContent

    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    content = EmailContent(
        subject="HTML Test",
        html="<h1>Hello</h1><p>From x8 test</p>",
    )

    result = await client.send(
        to=TEST_TO,
        content=content,
        from_email=TEST_FROM,
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.status is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_with_subject(provider_type: str, async_call: bool):
    from x8.communication.email_service import EmailContent

    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    content = EmailContent(
        subject="Test Subject from x8",
        text="Email body with custom subject",
    )

    result = await client.send(
        to=TEST_TO,
        content=content,
        from_email=TEST_FROM,
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_multiple_recipients(provider_type: str, async_call: bool):
    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    recipients = ["recipient1@example.com", "recipient2@example.com"]

    result = await client.send(
        to=recipients,
        content="Hello to multiple recipients",
        from_email=TEST_FROM,
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert len(result.to) == 2
    assert result.to == recipients


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_with_cc_and_bcc(provider_type: str, async_call: bool):
    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.send(
        to=TEST_TO,
        content="Email with CC and BCC",
        from_email=TEST_FROM,
        cc="cc@example.com",
        bcc="bcc@example.com",
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_with_reply_to(provider_type: str, async_call: bool):
    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.send(
        to=TEST_TO,
        content="Email with reply-to",
        from_email=TEST_FROM,
        reply_to="replyto@example.com",
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_with_attachment(provider_type: str, async_call: bool):
    from x8.communication.email_service import EmailAttachment, EmailContent

    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    content = EmailContent(
        subject="Email with Attachment",
        text="Please see the attached file.",
    )

    attachment = EmailAttachment(
        filename="test.txt",
        content="This is test file content",
        content_type="text/plain",
    )

    result = await client.send(
        to=TEST_TO,
        content=content,
        from_email=TEST_FROM,
        attachments=[attachment],
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_with_text_and_html(provider_type: str, async_call: bool):
    from x8.communication.email_service import EmailContent

    client = EmailServiceSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    content = EmailContent(
        subject="Multipart Email",
        text="Plain text version",
        html="<h1>HTML version</h1>",
    )

    result = await client.send(
        to=TEST_TO,
        content=content,
        from_email=TEST_FROM,
    )

    assert isinstance(result, EmailSendResult)
    assert result.id is not None
    assert result.provider == "sendgrid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_missing_sender_raises_error(
    provider_type: str, async_call: bool
):
    from x8.communication.email_service import EmailService

    # Create component without from_email
    component = EmailService(
        __unpack__=True,
        __provider__=dict(
            type=provider_type,
            parameters={"api_key": "test-key"},
        ),
    )

    with pytest.raises(BadRequestError):
        if async_call:
            await component.asend(to=TEST_TO, content="No sender")
        else:
            component.send(to=TEST_TO, content="No sender")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        EmailServiceProvider.SENDGRID,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_send_missing_content_raises_error(
    provider_type: str, async_call: bool
):
    from x8.communication.email_service import EmailContent, EmailService

    component = EmailService(
        __unpack__=True,
        __provider__=dict(
            type=provider_type,
            parameters={"api_key": "test-key"},
        ),
    )

    content = EmailContent(subject="No body")

    with pytest.raises(BadRequestError):
        if async_call:
            await component.asend(
                to=TEST_TO, content=content, from_email=TEST_FROM
            )
        else:
            component.send(to=TEST_TO, content=content, from_email=TEST_FROM)
