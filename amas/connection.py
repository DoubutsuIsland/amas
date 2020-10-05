from multiprocessing import Pipe
from multiprocessing.connection import Connection
from typing import Any, Dict, Iterable, List, Optional, Tuple

Address = str
Message = Any
Mail = Tuple[Address, Message]


class Sender(object):
    def __init__(self, conn: Connection) -> None:
        self.__conn = conn
        return None

    def __del__(self) -> None:
        if not self.__conn.closed:
            self.__conn.close()
        return None

    def send(self, addr: Address, message: Message) -> None:
        self.__conn.send((addr, message))
        return None


class Receiver(object):
    def __init__(self, conn: Connection) -> None:
        self.__conn = conn
        return None

    def __del__(self) -> None:
        if not self.__conn.closed:
            self.__conn.close()
        return None

    def recv(self) -> Mail:
        return self.__conn.recv()

    def poll(self, timeout: Optional[float] = None) -> bool:
        return self.__conn.poll(timeout)


SendMap = Dict[Address, Sender]
RecvMap = Dict[Address, Receiver]
MailBox = List[Mail]


class Connector(object):
    def __init__(self, addr: Address):
        self.addr = addr
        self.__receiver: Optional[Receiver] = None
        self.__senders: Optional[SendMap] = None
        self.__robs: Optional[Receiver] = None
        self.__sobs: Optional[Sender] = None
        return None

    def sign_up(self, mailbox: Receiver, dest: SendMap,
                robs: Receiver) -> None:
        self.__receiver = mailbox
        self.__senders = dest
        self.__robs = robs
        return None

    def get_destination(self) -> List[Address]:
        return list(self.__senders.keys())

    def send_to(self, addr: Address, message: Message) -> None:
        self.__senders[addr].send(self.addr, message)

    def recv(self) -> Mail:
        return self.__receiver.recv()

    def recv_from_observer(self) -> Mail:
        return self.__robs.recv()

    def poll(self, timeout: Optional[float] = None) -> bool:
        return self.__receiver.poll(timeout)

    def poll_from_observer(self, timeout: Optional[float] = None) -> bool:
        return self.__robs.poll(timeout)


class Register(object):
    def __init__(self, clients: Iterable[Connector]) -> None:
        self.__recv: RecvMap = dict()
        self.__send: SendMap = dict()
        self.__robs: RecvMap = dict()
        self.__sobs: SendMap = dict()
        for client in clients:
            addr = client.addr
            recv, send = Pipe()
            robs, sobs = Pipe()
            self.__recv[addr] = Receiver(recv)
            self.__send[addr] = Sender(send)
            self.__sobs[addr] = Sender(sobs)
            self.__robs[addr] = Receiver(robs)
            if addr == "OBSERVER":
                client.sign_up(self.__recv[addr], self.__sobs,
                               self.__robs[addr])
            else:
                client.sign_up(self.__recv[addr], self.__send,
                               self.__robs[addr])
        return None

    def register(self, client: Connector) -> None:
        addr = client.addr
        recv, send = Pipe()
        robs, sobs = Pipe()
        self.__recv[addr] = Receiver(recv)
        self.__send[addr] = Sender(send)
        self.__sobs[addr] = Sender(sobs)
        self.__robs[addr] = Receiver(robs)
        if addr == "OBSERVER":
            client.sign_up(self.__recv[addr], self.__sobs, self.__robs[addr])
        else:
            client.sign_up(self.__recv[addr], self.__send, self.__robs[addr])
        return None
