import asyncio
import functools
import random
import string
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import aiohttp
import loguru
import orjson

logger = loguru.logger

PKG_MAGIC = b"  V2"
RDY_SIZE = 500
MSG_SIZE = 1024 * 1024  # default is 1Mb
TSK_OVER = 0.025  # unit: second


async def public_ip():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.ip.sb/ip") as response:
            return await response.text()


@dataclass
class NSQInterface:
    host: str = "127.0.0.1"
    port: int = 4070

    topic: str = ""
    channel: str = ""

    reader: Optional[StreamReader] = None
    writer: Optional[StreamWriter] = None

    is_connect = False

    sent: int = 0
    cost: int = 0
    rdy: int = RDY_SIZE

    tasks: Dict[str, asyncio.Task] = field(default_factory=dict)

    _connect_is_broken = False

    async def send_identify(self):
        info = orjson.dumps(
            {
                "hostname": await public_ip(),
                "client_id": "".join(random.choice(string.ascii_lowercase) for _ in range(8)),
                "user_agent": "aonsq.py/0.0.4",
                # "deflate": True,
                # "deflate_level": 5,
                "msg_timeout": 30 * 1000,  # 30s
            }
        )

        await self.write(f"IDENTIFY\n".encode() + len(info).to_bytes(4, "big") + info)

        resp = await self.read()

        # error/exception
        if resp is None:
            return False

        if resp[4:] == b"OK":
            return True

        logger.warning(f"identify error:{resp[4:].decode()}")
        return False

    async def connect(self):
        reader, writer = await asyncio.open_connection(self.host, self.port, limit=MSG_SIZE)

        self.reader = reader
        self.writer = writer

        await self.write(b"  V2")

        # d(f"nsq:{self.host}:{self.port}")

        if not await self.send_identify():
            logger.warning("send identify error")
            self.is_connect = False
            return

        self.is_connect = True
        self._connect_is_broken = False

        loop = asyncio.get_event_loop()

        self.tasks["tx"] = loop.create_task(self._tx_worker())
        self.tasks["rx"] = loop.create_task(self._rx_worker())
        self.tasks["sub"] = loop.create_task(self._sub_worker())
        self.tasks["watchdog"] = loop.create_task(self._watchdog())

        # for name, task in self.tasks.items():
        #     task.add_done_callback(functools.partial(logger.debug, f"task {name} is done"))

    async def disconnect(self):
        # cancel the tasks
        for name, task in self.tasks.items():
            if task is None:
                continue

            if task.cancelled():
                continue

            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                # logger.error(f"task {name} cancel error")
                pass

            logger.debug(f"task {name} is canceled")

        # reset
        self.tasks.clear()

        if self.writer is not None:
            self.writer.close()

            try:  # ignore the connection error
                await self.writer.wait_closed()
            except ConnectionError:
                pass

            self.writer = None

            logger.debug(f"connection writer is closed")

        if self.reader is not None:
            try:  # ignore the connection error
                self.reader.set_exception(ConnectionAbortedError())
            except ConnectionError:
                pass

            self.reader = None

            logger.debug(f"connection reader is reset")

        self.is_connect = False

    async def write(self, msg: Union[str, bytes], wait=True) -> bool:
        if self._connect_is_broken:
            return False

        if isinstance(msg, str):
            self.writer.write(msg.encode())
        elif isinstance(msg, bytes):
            self.writer.write(msg)
        else:
            raise TypeError("invalid data type")

        try:
            await self.writer.drain()
        except AssertionError as e:
            logger.exception("writer assert error")

            if not self._connect_is_broken:
                self._connect_is_broken = True

            return False
        except ConnectionError:
            logger.error("write connection error")

            # logger.error(f"topic {self.topic}/{self.channel} connection error")
            if not self._connect_is_broken:
                self._connect_is_broken = True

            return False

        return True

    async def read(self, size=4) -> Optional[bytes]:
        try:
            raw = await self.reader.readexactly(size)
            size = int.from_bytes(raw, byteorder="big")

            return await self.reader.readexactly(size)

        except ConnectionError as exc:
            if not self._connect_is_broken:
                self._connect_is_broken = True
                logger.exception(f"read error")

            return None

        except asyncio.streams.IncompleteReadError as exc:
            if not self._connect_is_broken:
                self._connect_is_broken = True

                logger.exception("stream error")

            return None
