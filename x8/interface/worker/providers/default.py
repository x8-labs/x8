import multiprocessing
from typing import Any

from x8.core import (
    Component,
    Context,
    Operation,
    OperationParser,
    Provider,
    Response,
)

from .._operation import WorkerOperation


def _start(context, queue, component, max_wait_time, stop_event):
    while not stop_event.is_set():
        res = queue.__run__(
            Operation(
                name="pull",
                args=dict(config=dict(max_wait_time=max_wait_time)),
            ),
            context,
        )
        if len(res.result) > 0:
            job = res.result[0]
            component.__run__(
                job.value["operation"],
                job.value["context"],
            )
            queue.__run__(Operation(name="ack", args=dict(key=job.key)))


async def _astart(context, queue, component, max_wait_time, stop_event):
    while not stop_event.is_set():
        res = await queue.__arun__(
            Operation(
                name="pull",
                args=dict(config=dict(max_wait_time=max_wait_time)),
            ),
            context,
        )
        if len(res.result) > 0:
            job = res.result[0]
            component.__run__(
                job.value["operation"],
                job.value["context"],
            )
            await queue.__arun__(Operation(name="ack", args=dict(key=job.key)))


class Default(Provider):
    queue: Component
    component: Component
    workers: int
    max_wait_time: int
    nparams: dict[str, Any]

    _stop_event: Any
    _processes: list

    def __init__(
        self,
        queue: Component,
        component: Component,
        workers: int = 1,
        max_wait_time: int = 30,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            queue:
                Queue component from which jobs are fetched for processing.
            component:
                Component to host as API.
            workers:
                Number of worker processes.
            max_wait_time:
                Maximum wait time to receive item from the queue.
                If time expires, the worker will automatically check again
                unless the workers are stopped.
            nparams:
                Native parameters to FastAPI and uvicorn client.
        """
        self.queue = queue
        self.component = component
        self.workers = workers
        self.nparams = nparams
        self.max_wait_time = max_wait_time
        self._stop_event = multiprocessing.Event()
        self._processes = []
        print("Started worker")

    def __setup__(self, context: Context | None = None) -> None:
        self.__setup__(context=context)

    async def __asetup__(self, context: Context | None = None) -> None:
        await self.__asetup__(context=context)

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Response[None]:
        op_parser = OperationParser(operation)
        if operation is None or op_parser.op_equals(WorkerOperation.START):
            if self.workers == 1:
                _start(
                    context,
                    self.queue,
                    self.component,
                    self.max_wait_time,
                    self._stop_event,
                )
            else:
                for _ in range(self.workers):
                    print("starting process")
                    p = multiprocessing.Process(
                        target=_start,
                        kwargs=dict(
                            context=(
                                context.to_dict()
                                if context is not None
                                else None
                            ),
                            queue=self.queue,
                            component=self.component,
                            max_wait_time=self.max_wait_time,
                            stop_event=self._stop_event,
                        ),
                    )
                    p.start()
                    self._processes.append(p)
                for p in self._processes:
                    p.join()
        elif op_parser.op_equals(WorkerOperation.STOP):
            self._stop(context)
        for p in self._processes:
            p.join()
        return Response(result=None)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Response[None]:
        op_parser = OperationParser(operation)
        if operation is None or op_parser.op_equals(WorkerOperation.START):
            if self.workers == 1:
                await _astart(
                    context,
                    self.queue,
                    self.component,
                    self.max_wait_time,
                    self._stop_event,
                )
            else:
                for _ in range(self.workers):
                    p = multiprocessing.Process(
                        target=_astart,
                        kwargs=dict(
                            context=(
                                context.to_dict()
                                if context is not None
                                else None
                            ),
                            queue=self.queue,
                            component=self.component,
                            max_wait_time=self.max_wait_time,
                            stop_event=self._stop_event,
                        ),
                    )
                    p.start()
                    self._processes.append(p)
                for p in self._processes:
                    p.join()
        elif op_parser.op_equals(WorkerOperation.STOP):
            self._stop(context)
        return Response(result=None)

    def _stop(self, context):
        self._stop_event.set()
