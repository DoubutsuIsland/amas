import asyncio
from multiprocessing import Process
from typing import Coroutine, Iterable, List

from amas.agent import Agent


class Environment(object):
    def __init__(self, agents: Iterable[Agent]) -> None:
        self.__agents = agents
        self.__loop = asyncio.get_event_loop()
        self.__proc = None
        return None

    def __gather(self) -> None:
        tasks: List[Coroutine] = []
        [tasks.extend(agent.run()) for agent in self.__agents]
        g = asyncio.gather(*tuple(tasks))
        self.__loop.run_until_complete(g)
        return None

    def run(self) -> None:
        self.__gather()

    def parallelize(self) -> None:
        self.__proc = Process(target=self.__gather, args=())
        self.__proc.start()
        return None

    def join(self, timeout: float = None) -> None:
        if self.__proc is not None:
            self.__proc.join(timeout)
        return None

    def kill(self):
        if self.__proc is not None:
            self.__proc.kill()
        return None
