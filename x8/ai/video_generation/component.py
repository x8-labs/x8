from typing import Any, AsyncIterator, Iterator, List, Literal, overload

from x8.core import Component, Response, operation

from ._models import (
    ImageData,
    KeyFrame,
    Reference,
    VideoGenerationResult,
    VideoSize,
)


class VideoGeneration(Component):
    @overload
    def generate(
        self,
        prompt: str | None = None,
        *,
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[VideoGenerationResult]:
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
    def generate(
        self,
        prompt: str | None = None,
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[VideoGenerationResult]]:
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
    def generate(
        self,
        prompt: str | None = None,
        *,
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
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}",
            "method": "GET",
            "status": 200,
        }
    )
    def get(self, id: str, **kwargs: Any) -> Response[VideoGenerationResult]:
        """
        Get the status and details of a video generation task.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "GET",
            "status": 200,
        }
    )
    def list(self, **kwargs: Any) -> Response[List[VideoGenerationResult]]:
        """
        List all video generation tasks.

        Args:
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}",
            "method": "DELETE",
            "status": 204,
        }
    )
    def delete(self, id: str, **kwargs: Any) -> Response[None]:
        """
        Delete a video generation task.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}/content",
            "method": "GET",
            "status": 200,
            "response": {
                "media_type": "video/mp4",
            },
        }
    )
    def download(self, id: str, **kwargs: Any) -> Iterator[bytes]:
        """
        Download the generated video.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @overload
    async def agenerate(
        self,
        prompt: str | None = None,
        *,
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> Response[VideoGenerationResult]:
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
    async def agenerate(
        self,
        prompt: str | None = None,
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
    ) -> AsyncIterator[Response[VideoGenerationResult]]:
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
            "response": {"type": "stream"},
        }
    )
    async def agenerate(
        self,
        prompt: str | None = None,
        *,
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
        """
        Generate a video from input.

        Args:
            prompt:
                The text prompt to generate the video.
            image:
                An image to guide the video generation.
            references:
                List of reference images to guide the video generation.
            key_frames:
                List of key frames to include in the video.
            negative_prompt:
                The negative text prompt to avoid in the video.
            audio:
                Whether to include audio in the generated video.
            duration:
                The duration of the video in seconds.
            size:
                The size of the video.
            samples:
                The number of video samples to generate.
            seed:
                The random seed for reproducibility.
            quality:
                The quality setting for the video generation.
            poll:
                Whether to poll for completion.
            poll_interval:
                The interval (in seconds) between polling attempts.
            stream:
                Whether to stream the response.
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}",
            "method": "GET",
            "status": 200,
        }
    )
    async def aget(
        self, id: str, **kwargs: Any
    ) -> Response[VideoGenerationResult]:
        """
        Get the status and details of a video generation task.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "GET",
            "status": 200,
        }
    )
    async def alist(
        self, **kwargs: Any
    ) -> Response[List[VideoGenerationResult]]:
        """
        List all video generation tasks.

        Args:
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}",
            "method": "DELETE",
            "status": 204,
        }
    )
    async def adelete(self, id: str, **kwargs: Any) -> Response[None]:
        """
        Delete a video generation task.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/{id}/content",
            "method": "GET",
            "status": 200,
            "response": {
                "media_type": "video/mp4",
            },
        }
    )
    async def adownload(self, id: str, **kwargs: Any) -> AsyncIterator[bytes]:
        """
        Download the generated video.

        Args:
            id:
                The ID of the video generation task.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError
