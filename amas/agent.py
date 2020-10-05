import asyncio
from functools import partial
from time import perf_counter
from typing import Any, Awaitable, Callable, Coroutine, List, Optional

from amas.connection import Address, Connector, Mail, Message

OBSERVER = "OBSERVER"


class NotAssignedError(Exception):
    pass


class NotWorkingError(Exception):
    pass


class Agent(Connector):
    def __init__(self, addr: Address) -> None:
        super().__init__(addr)
        self.tasks: List[Callable] = []
        self.__working = False
        self.__clock_speed = 0.0001
        return None

    def assign_task(self, task: Callable[..., Awaitable], **kwargs) -> 'Agent':
        task = partial(task, **kwargs)
        self.tasks.append(task)
        return self

    def apply_args(self, index: int = 0, **kwargs) -> 'Agent':
        task = partial(self.tasks[index], **kwargs)
        self.tasks[index] = task
        return self

    def start(self) -> None:
        self.__working = True
        return None

    def finish(self) -> None:
        if not self.__working:
            raise NotWorkingError("Agent is not working")
        self.__working = False
        return None

    def working(self) -> bool:
        return self.__working

    def set_clock(self, speed: float) -> None:
        if speed > 0.01:
            raise ValueError("speed must be greater than 0.01")
        self.__clock_speed = speed

    async def sleep(self, delay: float) -> None:
        t = perf_counter() + delay
        while (perf_counter() - t) <= self.__clock_speed:
            if not self.working():
                raise NotWorkingError
            await asyncio.sleep(self.__clock_speed)
        return None

    async def call_async(self, f: Callable, *args, **kwargs) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, f, *args, **kwargs)

    async def call_after(self, delay: float, fn: Callable, *args,
                         **kwargs) -> Any:
        await self.sleep(delay)
        return fn(*args, **kwargs)

    async def recv(self, t: Optional[float] = 1.0) -> Mail:
        while not await self.poll(t):
            if not self.working():
                raise NotWorkingError
        return super().recv()

    async def try_recv(self,
                       timeout: Optional[float] = None) -> Optional[Mail]:
        if not await self.poll(timeout):
            if not self.working():
                raise NotWorkingError
            return None
        return super().recv()

    async def recv_from_observer(self, t: Optional[float] = 1.0) -> Mail:
        while not await self.poll_from_observer(t):
            if not self.working():
                raise NotWorkingError
        return super().recv_from_observer()

    async def try_recv_from_observer(self,
                                     timeout: Optional[float] = None
                                     ) -> Optional[Mail]:
        if not await self.poll_from_observer(timeout):
            if not self.working():
                raise NotWorkingError
            return None
        return super().recv_from_observer()

    async def poll(self, timeout: Optional[float] = None) -> bool:
        return await self.call_async(super().poll, timeout)

    async def poll_from_observer(self,
                                 timeout: Optional[float] = None) -> bool:
        return await self.call_async(super().poll_from_observer, timeout)

    def run(self, *args, **kwargs) -> List[Coroutine]:
        if not len(self.tasks) > 0:
            raise NotAssignedError
        self.start()
        tasks = [task(self) for task in self.tasks]
        return tasks


class Observer(Agent):
    def __init__(self) -> None:
        super().__init__(OBSERVER)
        self.tasks: List[Callable] = []
        self.__working = False
        return None

    def send_all(self, message: Message) -> None:
        [self.send_to(addr, message) for addr in self.get_destination()]
        return None
