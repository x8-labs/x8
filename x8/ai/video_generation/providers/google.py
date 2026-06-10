import asyncio
import base64
import time
from typing import Any, AsyncIterator, Iterator, List

import google.auth
import google.auth.transport.requests
import httpx

from x8._common.google_provider import GoogleProvider
from x8.content.image import ImageData
from x8.content.video import VideoData
from x8.core import Response
from x8.core.exceptions import BadRequestError, NotFoundError

from .._models import (
    KeyFrame,
    Reference,
    Usage,
    VideoGenerationResult,
    VideoSize,
)


class Google(GoogleProvider):
    project: str | None
    location: str
    model: str
    nparams: dict[str, Any] | None

    _credentials: Any
    _generate_url: str
    _poll_url: str
    _full_name_prefix: str

    def __init__(
        self,
        project: str | None = None,
        location: str = "us-central1",
        model: str = "veo-3.1-fast-generate-001",
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        self.project = project
        self.location = location
        self.model = model
        self.nparams = nparams
        self._credentials = None
        self._generate_url = self._get_generate_url()
        self._poll_url = self._get_poll_url()
        self._full_name_prefix = self._get_full_name_prefix()
        super().__init__(
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def _get_token(self) -> str:
        if not self._credentials:
            self._credentials = (
                self._get_credentials() or google.auth.default()
            )
            self._credentials, _ = google.auth.default()
            request = google.auth.transport.requests.Request()
            self._credentials.refresh(request)

        if self._credentials:
            return self._credentials.token

        raise BadRequestError("Unable to obtain access token")

    def generate(
        self,
        prompt: str | None = None,
        *,
        model: str | None = None,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        poll: bool = False,
        poll_interval: float = 5.0,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[VideoGenerationResult]
        | Iterator[Response[VideoGenerationResult]]
    ):
        resolved_model = model or self.model
        generate_url = self._get_generate_url(model=resolved_model)
        poll_url = self._get_poll_url(model=resolved_model)
        billed_seconds = duration or 4.0
        body = self._convert_generate_args(
            prompt=prompt,
            model=model,
            image=image,
            references=references,
            key_frames=key_frames,
            negative_prompt=negative_prompt,
            audio=audio,
            duration=duration,
            size=size,
            samples=samples,
            seed=seed,
            quality=quality,
            nconfig=nconfig,
            **kwargs,
        )
        headers = self._get_headers()
        res = httpx.post(generate_url, json=body, headers=headers).json()
        if res.get("error"):
            return Response(
                result=self._build_result(
                    id="",
                    status="failed",
                    error=res["error"].get("message", "Unknown error"),
                    billed_seconds=billed_seconds,
                    model=resolved_model,
                )
            )

        id = res.get("name").split("/")[-1]

        if stream:

            def _poll_iter() -> Iterator[Response[VideoGenerationResult]]:
                yield Response(
                    result=self._build_result(
                        id=id,
                        status="queued",
                        billed_seconds=billed_seconds,
                    )
                )

                operation_name = res["name"]
                poll_body = {"operationName": operation_name}

                while True:
                    poll_res = httpx.post(
                        poll_url, headers=headers, json=poll_body
                    ).json()
                    if poll_res.get("error"):
                        yield Response(
                            result=self._build_result(
                                id=id,
                                status="failed",
                                error=res["error"].get(
                                    "message", "Unknown error"
                                ),
                                billed_seconds=billed_seconds,
                                model=resolved_model,
                            )
                        )
                        return

                    if poll_res.get("done"):
                        videos = self._parse_videos_response(
                            poll_res["response"]
                        )
                        yield Response(
                            result=self._build_result(
                                id=id,
                                status="completed",
                                videos=videos,
                                billed_seconds=billed_seconds,
                                model=resolved_model,
                            )
                        )
                        return
                    yield Response(
                        result=self._build_result(
                            id=id,
                            status="in_progress",
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                    time.sleep(poll_interval)

            return _poll_iter()

        if poll:
            operation_name = res["name"]
            poll_body = {"operationName": operation_name}
            while True:
                poll_res = httpx.post(
                    poll_url, headers=headers, json=poll_body
                ).json()
                if poll_res.get("error"):
                    return Response(
                        result=self._build_result(
                            id=id,
                            status="failed",
                            error=res["error"].get("message", "Unknown error"),
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                if poll_res.get("done"):
                    return Response(
                        result=self._build_result(
                            id=id,
                            status="completed",
                            videos=self._parse_videos_response(
                                poll_res["response"]
                            ),
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                time.sleep(poll_interval)
        return Response(
            result=self._build_result(
                id=id,
                status="queued",
                billed_seconds=billed_seconds,
                model=resolved_model,
            )
        )

    def get(self, id: str, **kwargs: Any) -> Response[VideoGenerationResult]:
        body = {"operationName": self._get_full_name(id)}
        headers = self._get_headers()
        res = httpx.post(self._get_poll_url(), headers=headers, json=body)
        if res.status_code == 404:
            raise NotFoundError(f"Video generation with ID {id} not found.")
        res_json = res.json()
        operation_done = res_json.get("done", False)
        if not operation_done:
            return Response(
                result=self._build_result(id=id, status="in_progress")
            )
        return Response(result=self._build_result(id=id, status="completed"))

    def download(self, id: str, **kwargs: Any) -> Iterator[bytes]:
        body = {"operationName": self._get_full_name(id)}
        headers = self._get_headers()
        res = httpx.post(self._get_poll_url(), headers=headers, json=body)
        if res.status_code == 404:
            raise NotFoundError(f"Video generation with ID {id} not found.")
        res_json = res.json()
        if not res_json.get("done", False):
            raise NotFoundError(
                f"Video generation with ID {id} is not completed yet."
            )
        base_64_video = res_json["response"]["videos"][0]["bytesBase64Encoded"]
        video = base64.b64decode(base_64_video)
        return self._bytes_to_iterator(video)

    def list(self, **kwargs: Any) -> Response[List[VideoGenerationResult]]:
        return Response(result=[])

    def delete(self, id: str, **kwargs: Any) -> Response[None]:
        return Response(result=None)

    async def agenerate(
        self,
        prompt: str | None = None,
        *,
        model: str | None = None,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        poll: bool = False,
        poll_interval: float = 5.0,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[VideoGenerationResult]
        | AsyncIterator[Response[VideoGenerationResult]]
    ):
        resolved_model = model or self.model
        generate_url = self._get_generate_url(model=resolved_model)
        poll_url = self._get_poll_url(model=resolved_model)
        billed_seconds = duration or 4.0
        body = self._convert_generate_args(
            prompt=prompt,
            model=model,
            image=image,
            references=references,
            key_frames=key_frames,
            negative_prompt=negative_prompt,
            audio=audio,
            duration=duration,
            size=size,
            samples=samples,
            seed=seed,
            quality=quality,
            nconfig=nconfig,
            **kwargs,
        )
        headers = self._get_headers()
        async with httpx.AsyncClient() as client:
            r = await client.post(generate_url, json=body, headers=headers)
            res = r.json()
        if res.get("error"):
            return Response(
                result=self._build_result(
                    id="",
                    status="failed",
                    error=res["error"].get("message", "Unknown error"),
                    billed_seconds=billed_seconds,
                    model=resolved_model,
                )
            )
        id = res.get("name").split("/")[-1]
        if stream:

            async def _poll_aiter() -> (
                AsyncIterator[Response[VideoGenerationResult]]
            ):
                yield Response(
                    result=self._build_result(
                        id=id,
                        status="queued",
                        billed_seconds=billed_seconds,
                    )
                )
                operation_name = res["name"]
                poll_body = {"operationName": operation_name}

                while True:
                    async with httpx.AsyncClient() as client:
                        r = await client.post(
                            poll_url, headers=headers, json=poll_body
                        )
                        poll_res = r.json()
                    if poll_res.get("error"):
                        yield Response(
                            result=self._build_result(
                                id=id,
                                status="failed",
                                error=res["error"].get(
                                    "message", "Unknown error"
                                ),
                                billed_seconds=billed_seconds,
                                model=resolved_model,
                            )
                        )
                        return
                    if poll_res.get("done"):
                        videos = self._parse_videos_response(
                            poll_res["response"]
                        )
                        yield Response(
                            result=self._build_result(
                                id=id,
                                status="completed",
                                videos=videos,
                                billed_seconds=billed_seconds,
                                model=resolved_model,
                            )
                        )
                        return
                    yield Response(
                        result=self._build_result(
                            id=id,
                            status="in_progress",
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                    await asyncio.sleep(poll_interval)

            return _poll_aiter()

        if poll:
            operation_name = res["name"]
            poll_body = {"operationName": operation_name}
            while True:
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        poll_url, headers=headers, json=poll_body
                    )
                    poll_res = r.json()
                if poll_res.get("error"):
                    return Response(
                        result=self._build_result(
                            id=id,
                            status="failed",
                            error=res["error"].get("message", "Unknown error"),
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                if poll_res.get("done"):
                    res = Response(
                        result=self._build_result(
                            id=id,
                            status="completed",
                            videos=self._parse_videos_response(
                                poll_res["response"]
                            ),
                            billed_seconds=billed_seconds,
                            model=resolved_model,
                        )
                    )
                    return res
                time.sleep(poll_interval)
        return Response(
            result=self._build_result(
                id=id,
                status="queued",
                billed_seconds=billed_seconds,
                model=resolved_model,
            )
        )

    async def aget(
        self, id: str, **kwargs: Any
    ) -> Response[VideoGenerationResult]:
        body = {"operationName": self._get_full_name(id)}
        headers = self._get_headers()
        async with httpx.AsyncClient() as client:
            res = await client.post(
                self._get_poll_url(), headers=headers, json=body
            )
        if res.status_code == 404:
            raise NotFoundError(f"Video generation with ID {id} not found.")
        res_json = res.json()
        operation_done = res_json.get("done", False)
        if not operation_done:
            return Response(
                result=self._build_result(id=id, status="in_progress")
            )
        return Response(result=self._build_result(id=id, status="completed"))

    async def alist(
        self, **kwargs: Any
    ) -> Response[List[VideoGenerationResult]]:
        return Response(result=[])

    async def adelete(self, id: str, **kwargs: Any) -> Response[None]:
        return Response(result=None)

    async def adownload(self, id: str, **kwargs: Any) -> AsyncIterator[bytes]:
        body = {"operationName": self._get_full_name(id)}
        headers = self._get_headers()
        async with httpx.AsyncClient() as client:
            res = await client.post(
                self._get_poll_url(), headers=headers, json=body
            )
        if res.status_code == 404:
            raise NotFoundError(f"Video generation with ID {id} not found.")
        res_json = res.json()
        if not res_json.get("done", False):
            raise NotFoundError(
                f"Video generation with ID {id} is not completed yet."
            )
        base_64_video = res_json["response"]["videos"][0]["bytesBase64Encoded"]
        video = base64.b64decode(base_64_video)
        return self._bytes_to_async_iterator(video)

    def _convert_generate_args(
        self,
        prompt: str | None = None,
        model: str | None = None,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        args: dict[str, Any] = {}
        instance: dict[str, Any] = {}
        parameters: dict[str, Any] = {}
        if prompt:
            instance["prompt"] = prompt
        if image:
            instance["image"] = self._get_image(image)
        if key_frames and len(key_frames) > 0:
            instance["lastFrame"] = self._get_image(key_frames[-1].image)
        if references:
            arg_references = []
            for reference in references:
                arg_reference: dict[str, Any] = {}
                if reference.type:
                    arg_reference["type"] = reference.type
                if reference.image:
                    arg_reference["image"] = self._get_image(reference.image)
                arg_references.append(arg_reference)
            instance["referenceImages"] = arg_references
        args["instances"] = [instance]
        args["parameters"] = parameters
        if not size:
            size = "1280x720"
        if size == "1280x720":
            parameters["resolution"] = "720p"
            parameters["aspectRatio"] = "16:9"
        elif size == "720x1280":
            parameters["resolution"] = "720p"
            parameters["aspectRatio"] = "9:16"
        elif size == "1920x1080":
            parameters["resolution"] = "1080p"
            parameters["aspectRatio"] = "16:9"
        elif size == "1080x1920":
            parameters["resolution"] = "1080p"
            parameters["aspectRatio"] = "9:16"
        if duration:
            parameters["duration"] = duration
        else:
            parameters["duration"] = 4
        if audio is not None:
            parameters["audio"] = audio
        if negative_prompt:
            parameters["negativePrompt"] = negative_prompt
        if samples:
            parameters["sampleCount"] = samples
        if seed:
            parameters["seed"] = seed
        if quality:
            parameters["compressionQuality"] = quality
        return args

    def _get_full_name(self, id: str, model: str | None = None) -> str:
        return f"{self._get_full_name_prefix(model=model)}/{id}"

    def _get_generate_url(self, model: str | None = None) -> str:
        return f"{self._get_base_url(model=model)}:predictLongRunning"

    def _get_poll_url(self, model: str | None = None) -> str:
        return f"{self._get_base_url(model=model)}:fetchPredictOperation"

    def _get_base_url(self, model: str | None = None) -> str:
        project = self.project or self._get_default_project()
        resolved_model = model or self.model
        return (
            f"https://{self.location}-aiplatform.googleapis.com/v1/"
            f"projects/{project}/locations/{self.location}/publishers/google/"
            f"models/{resolved_model}"
        )

    def _get_full_name_prefix(self, model: str | None = None) -> str:
        project = self.project or self._get_default_project()
        resolved_model = model or self.model
        return (
            f"projects/{project}/locations/{self.location}/publishers/google/"
            f"models/{resolved_model}/operations"
        )

    def _get_headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        return headers

    def _get_image(self, image: ImageData) -> dict[str, Any]:
        args: dict[str, Any] = {}
        if image.source:
            args["gcrUri"] = image.source
        elif isinstance(image.content, str):
            args["bytesBase64Encoded"] = image.content
        elif isinstance(image.content, bytes):
            args["bytesBase64Encoded"] = base64.b64encode(
                image.content
            ).decode("utf-8")
        else:
            raise BadRequestError("Image data not provided.")
        if image.media_type:
            args["mimeType"] = image.media_type
        return args

    def _parse_videos_response(self, res: dict[str, Any]) -> List[VideoData]:
        results: List[VideoData] = []
        for video_data in res.get("videos", []):
            video = VideoData(
                content=video_data.get("bytesBase64Encoded"),
                media_type=video_data.get("mimeType"),
            )
            results.append(video)
        return results

    def _build_result(
        self,
        id: str,
        status: str,
        *,
        error: str | None = None,
        videos: List[VideoData] | None = None,
        billed_seconds: float | None = None,
        model: str | None = None,
    ) -> VideoGenerationResult:
        return VideoGenerationResult(
            id=id,
            model=model or self.model,
            duration=billed_seconds,
            status=status,
            error=error,
            usage=self._build_usage(billed_seconds),
            videos=videos,
        )

    def _build_usage(self, billed_seconds: float | None) -> Usage | None:
        if billed_seconds is None:
            return None
        return Usage(billed_seconds=billed_seconds)

    def _bytes_to_iterator(
        self, data: bytes, chunk_size: int = 1024
    ) -> Iterator[bytes]:
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]  # noqa: E203

    async def _bytes_to_async_iterator(
        self, data: bytes, chunk_size: int = 1024
    ) -> AsyncIterator[bytes]:
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]  # noqa: E203
            await asyncio.sleep(0)
