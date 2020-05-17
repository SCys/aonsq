import asyncio
import functools
import random
import string
import traceback
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import aiohttp
import loguru
import orjson

from .nsq_message import NSQMessage

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
class NSQBasic:
    host: str = "127.0.0.1"
    port: int = 4070

    topic: str = ""
    channel: str = ""
    # stats: Dict[str, int] = field(default_factory=dict)

    reader: Optional[StreamReader] = None
    writer: Optional[StreamWriter] = None

    is_connect = False

    # sub options
    rx_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=RDY_SIZE * 2))
    tx_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=RDY_SIZE * 5))
    handler: Optional[Callable[[Any], Awaitable[bool]]] = None

    sent: int = 0
    cost: int = 0
    rdy: int = RDY_SIZE

    tasks: Dict[str, asyncio.Task] = field(default_factory=dict)

    _connect_is_broken = False

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

        for name, task in self.tasks.items():
            if self.topic and self.channel:
                func = functools.partial(logger.debug, f"topic {self.topic}/{self.channel} task {name} is done")
            else:
                func = functools.partial(logger.debug, f"task {name} is done")

            task.add_done_callback(func)

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
                logger.error(f"task {name} cancel error")
            else:
                logger.info(f"task {name} is canceled")

        # reset
        self.tasks.clear()

        if self.writer is not None:
            self.writer.close()

            await self.writer.drain()
            await self.writer.wait_closed()

            self.writer = None
            self.reader = None

            logger.info(f"connection is closed")

        try:
            self.reader.set_exception(ConnectionAbortedError())
        except:
            pass

        self.is_connect = False

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

    async def send_sub(self):
        await self.write(f"SUB {self.topic} {self.channel}\n")

    async def send_rdy(self):
        await self.write(f"RDY {self.rdy}\n")

    async def send_pub(self, topic, msg):
        raw = f"PUB {topic}\n".encode() + len(msg).to_bytes(4, "big") + msg

        if await self.write(raw):
            self.sent += 1
            return False

        return True

    async def async_task(self, handler, msg):
        # tpCost = datetime.now()

        try:
            result = await self.handler(msg)
        except asyncio.TimeoutError as e:
            logger.error(f"topic {self.topic}/{self.channel} handler error:{e}")

            result = False

        # if (datetime.now() - tpCost).total_seconds() > TSK_OVER:  # cost more than task limit
        #     logger.debug(f"task with msg {msg.id} cost more than {TSK_OVER}s")

        try:
            if result:
                await self.write(f"FIN {msg.id}\n")
            else:
                await self.write(f"REQ {msg.id}\n")

        except ConnectionError as e:
            logger.warning(f"topic {self.topic}/{self.channel} fin/req with connection error")

            self._connect_is_broken = True

        self.rdy -= 1

        # if (datetime.now() - tpCost).total_seconds() > TSK_OVER * 2:  # cost more than task limit *2
        #     logger.debug(f"task with msg {msg.id} cost more than {TSK_OVER * 2}s")

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
            logger.error(f"topic assert error")

            if not self._connect_is_broken:
                self._connect_is_broken = True

            return False
        except ConnectionError:
            logger.error(f"topic {self.topic}/{self.channel} connection error")

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
                logger.error(f"read error:{str(exc)}")

            return None

        except asyncio.streams.IncompleteReadError as exc:
            if not self._connect_is_broken:
                self._connect_is_broken = True
                logger.error(f"stream error:{str(exc)}")

            return None

    async def _tx_worker(self):
        # logger.debug("tx worker is running")

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            if self.tx_queue.empty():
                await asyncio.sleep(0.1)  # 100ms
                continue

            topic, content = await self.tx_queue.get()
            await self.send_pub(topic, content)
            self.tx_queue.task_done()

    async def _rx_worker(self):
        # logger.debug("rx worker is running")

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            resp = await self.read()
            if resp is None:
                break

            frame_type = int.from_bytes(resp[:4], byteorder="big")
            frame_data = resp[4:]

            if frame_type == 0:
                if frame_data == b"OK":
                    continue

                # elif frame_data == b"CLOSE_WAIT":
                #     i("server return close wait")

                if frame_data == b"_heartbeat_":
                    await self.write(b"NOP\n")
                    continue

                logger.debug(f"response /{frame_data.decode()}/")
                continue

            elif frame_type == 1:  # error
                err_msg: str = frame_data.decode()

                # clear the rx_queue
                if err_msg.startswith("E_FIN_FAILED"):

                    messages = []
                    while not self.rx_queue.empty():
                        msg = await self.rx_queue.get()
                        messages.append(msg.id)
                        self.rx_queue.task_done()

                    for msg_id in messages:
                        await self.write(f"REQ {msg_id}\n")

                    # reset the rdy
                    self.rdy = 0

                    if self.topic and self.channel:
                        logger.debug(f"topic {self.topic}/{self.channel} rx queue is reset")
                        self._connect_is_broken = True
                        continue

                logger.warning(f"topic {self.topic}/{self.channel} logic error:{err_msg}")

                continue

            elif frame_type == 2:  # message
                # msg_raw = zlib.decompress(frame_data)
                msg_raw = frame_data

                msg = NSQMessage(
                    timestamp=int.from_bytes(msg_raw[:8], byteorder="big"),
                    attempts=int.from_bytes(msg_raw[8:10], byteorder="big"),
                    id=msg_raw[10:26].decode().strip(),
                    content=msg_raw[26:],
                )
                await self.rx_queue.put(msg)

                self.cost += 1

                if len(msg.id) < 16:
                    logger.debug(f"frame {frame_type} /{frame_data}/")

                continue

            logger.debug(f"frame {frame_type} /{frame_data}/")

    async def _sub_worker(self):
        # logger.debug("sub worker is running")

        tasks = []

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            if self.rdy <= 0:
                # if self.topic and self.channel:
                #     logger.debug(f"sub {self.topic}/{self.channel} cost:{self.cost}")

                if await self.write(f"RDY {RDY_SIZE}\n"):
                    self.rdy = RDY_SIZE

                await asyncio.sleep(TSK_OVER)
                continue

            if self.rx_queue.empty():
                await asyncio.sleep(TSK_OVER)
                continue

            # wait for anything
            if self.handler is None:
                msg = await self.rx_queue.get()

                await self.write(f"FIN {msg.id}\n")

                self.rdy -= 1
                self.rx_queue.task_done()
                continue

            while len(tasks) <= RDY_SIZE and not self.rx_queue.empty():
                msg = await self.rx_queue.get()

                task = asyncio.create_task(self.async_task(self.handler, msg))
                tasks.append(task)

                self.rx_queue.task_done()

            # wait for tasks
            if tasks:
                done, pending = await asyncio.wait(tasks, timeout=TSK_OVER)
                tasks = list(pending)
                # d(f"total {len(done)} tasks is done")

    async def _watchdog(self):
        # logger.debug("watchdog is running")

        while self.is_connect:
            await asyncio.sleep(1)

            if not self._connect_is_broken:
                continue

            # wait for other worker is closed
            await asyncio.sleep(5)

            if self.topic and self.channel:
                logger.debug(f"topic {self.topic}/{self.channel} will be reconnected")

            await self.disconnect()
            await asyncio.sleep(2)

            while True:
                try:
                    await self.connect()

                    if self.topic and self.channel:
                        await self.send_sub()
                        await self.send_rdy()

                    break

                except ConnectionAbortedError:
                    if self.topic and self.channel:
                        logger.exception(f"topic {self.topic}/{self.channel} reconnect error")
                    else:
                        logger.exception(f"topic reconnect error")

                    await asyncio.sleep(1)

                except ConnectionError:
                    if self.topic and self.channel:
                        logger.exception(f"topic {self.topic}/{self.channel} reconnect error")
                    else:
                        logger.exception(f"topic reconnect error")

                    await asyncio.sleep(1)
