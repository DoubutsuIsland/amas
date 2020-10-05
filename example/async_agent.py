import asyncio
from time import perf_counter

from amas.agent import Agent, NotWorkingError, Observer
from amas.connection import Register
from amas.env import Environment


async def send_after(agent: Agent, t: int, mess: str) -> None:
    while agent.working():
        await asyncio.sleep(t)
        await agent.send_to("bar", mess)
    return None


async def check_message(agent: Agent) -> None:
    while agent.working():
        try:
            mail = await agent.recv(timeout=2)
            if mail is None:
                continue
            sender, mess = mail
            print(f"{mess} from {sender}")
        except NotWorkingError:
            pass
    return None


async def quit(agent: Agent) -> None:
    while agent.working():
        _, mess = await agent.recv_from_observer()
        agent.finish()
    return None


async def kill(agent: Observer) -> None:
    await asyncio.sleep(5)
    await agent.send_all("q")


if __name__ == '__main__':
    a1 = Agent("foo")
    a2 = Agent("bar")
    a3 = Observer()

    rgist = Register([a1, a2, a3])

    a1.assign_task(send_after, t=1, mess="hello").assign_task(quit)
    a2.assign_task(check_message).assign_task(quit)
    a3.assign_task(kill)

    a1.apply_args(t=1, mess="hello")

    t = perf_counter()
    env = Environment([a1, a2, a3])
    env.run()
    print(perf_counter() - t)
