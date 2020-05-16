import asyncio
import random
import string
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
TSK_OVER = 0.05  # unit: second


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
    stats: Dict[str, int] = field(default_factory=dict)

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

    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    _busy_tx = False
    _busy_rx = False
    _busy_sub = False
    _busy_watchdog = False

    _connect_is_broken = False

    async def connect(self):
        reader, writer = await asyncio.open_connection(self.host, self.port, limit=MSG_SIZE)

        self.reader = reader
        self.writer = writer

        await self.write(b"  V2")

        self.is_connect = True
        self._connect_is_broken = False

        # d(f"nsq:{self.host}:{self.port}")

        if not await self.send_identify():
            logger.warning("send identify error")
            self.is_connect = False
            return

        if not self._busy_tx:
            asyncio.create_task(self._tx_worker())

        if not self._busy_rx:
            asyncio.create_task(self._rx_worker())

        if not self._busy_sub:
            asyncio.create_task(self._sub_worker())

        if not self._busy_watchdog:
            asyncio.create_task(self._watchdog())

    async def disconnect(self):
        if self.writer is not None:
            self.writer.close()
            self.reader.set_exception(ConnectionError())

            await self.writer.wait_closed()

            self.writer = None
            self.reader = None

        self.is_connect = False

    async def send_identify(self):
        info = orjson.dumps(
            {
                "hostname": await public_ip(),
                "client_id": "".join(random.choice(string.ascii_lowercase) for _ in range(8)),
                "user_agent": "aonsq.py/0.0.4",
                "deflate": True,
                "deflate_level": 5,
                "msg_timeout": 30 * 1000,  # 30s
            }
        )

        await self.write(f"IDENTIFY\n".encode() + len(info).to_bytes(4, "big") + info)

        resp = await self.read()
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
        try:
            await self.write(raw)
            self.sent += 1
        except ConnectionError as e:
            logger.warning(f"pub with connection error:{str(e)}")
            self._connect_is_broken = True
            return False

        return True

    async def _tx_worker(self):
        self._busy_tx = True

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

        logger.debug(f"tx worker is done")
        self._busy_tx = False

    async def _rx_worker(self):
        self._busy_rx = True

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            try:
                raw = await self.reader.readexactly(4)
            except ConnectionError as exc:
                logger.error(f"read head connection error:{str(exc)}")

                self._connect_is_broken = True
                break

            except asyncio.streams.IncompleteReadError as exc:
                logger.error(f"steam incomplete read error:{str(exc)}")

                self._connect_is_broken = True
                break

            size = int.from_bytes(raw, byteorder="big")
            if MSG_SIZE < size or size < 6:
                logger.warning(f"invalid size:{size} {raw}")
                continue

            try:
                resp = await self.reader.readexactly(size)
            except ConnectionError as exc:
                logger.error(f"read content connection error:{str(exc)}")

                self._connect_is_broken = True
                continue

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
                logger.warning(f"error response:{frame_data.decode()}")

                self._connect_is_broken = True
                break

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
                    logger.debug(f"frame {size} {frame_type} /{frame_data}/")

                continue

            logger.debug(f"frame {size} {frame_type} /{frame_data}/")

        logger.debug(f"rx worker is done")
        self._busy_rx = False

    async def _sub_worker(self):
        self._busy_sub = True

        tasks = []

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            if self.rx_queue.empty():
                await asyncio.sleep(TSK_OVER)
                continue

            if self.rdy <= 0:
                # d(f"sub {self.topic}/{self.channel} cost {self.cost}")
                self.rdy = RDY_SIZE

                try:
                    await self.write(f"RDY {self.rdy}\n")
                except ConnectionError as e:
                    logger.error(f"rdy with connection error:{e}")
                    self.reader.set_exception(e)

                    self._connect_is_broken = True
                    break

                continue

            if self.handler is None:
                msg = await self.rx_queue.get()

                try:
                    await self.write(f"FIN {msg.id}\n")
                except ConnectionError as e:
                    logger.error(f"fin with connection error:{e}")
                    self.reader.set_exception(e)

                    self._connect_is_broken = True
                    break

                self.rx_queue.task_done()

                self.rdy -= 1
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

        logger.debug(f"sub worker is done")
        self._busy_sub = False

    async def async_task(self, handler, msg):
        # tpCost = datetime.now()

        try:
            result = await self.handler(msg)
        except asyncio.TimeoutError as e:
            e(f"topic {self.topic}/{self.channel}/{msg.id} handler error:{e}")

            result = False

        # if (datetime.now() - tpCost).total_seconds() > TSK_OVER:  # cost more than task limit
        #     logger.debug(f"task with msg {msg.id} cost more than {TSK_OVER}s")

        try:
            if result:
                await self.write(f"FIN {msg.id}\n")
            else:
                await self.write(f"REQ {msg.id}\n")

        except ConnectionError as e:
            logger.warning(f"fin/req with connection error")
            self.reader.set_exception(e)

            self._connect_is_broken = True

        self.rdy -= 1

        # if (datetime.now() - tpCost).total_seconds() > TSK_OVER * 2:  # cost more than task limit *2
        #     logger.debug(f"task with msg {msg.id} cost more than {TSK_OVER * 2}s")

    async def _watchdog(self):
        self._busy_sub = True

        # stauts is recover or normal
        while self.is_connect:
            await asyncio.sleep(1)

            if self._connect_is_broken:
                await asyncio.sleep(5)

                logger.debug("nsq connection is being reconnected")

                while True:
                    try:
                        await self.disconnect()
                        await asyncio.sleep(1)

                        await self.connect()

                        await self.send_sub()
                        await self.send_rdy()
                        break
                    except ConnectionError as exc:
                        logger.error(f"nsq reconnect error:{exc}")

                        await asyncio.sleep(1)

        self._busy_watchdog = False

    async def write(self, msg: Union[str, bytes], wait=True):
        if self._connect_is_broken:
            return

        if isinstance(msg, str):
            self.writer.write(msg.encode())
        elif isinstance(msg, bytes):
            self.writer.write(msg)
        else:
            raise TypeError("invalid data type")

        try:
            await self.writer.drain()
        except AssertionError as e:
            logger.error(f"assert error:{str(e)}")
            if not self._connect_is_broken:
                self._connect_is_broken = True
        except ConnectionError:
            self._connect_is_broken = True

    async def read(self, size=4):
        raw = await self.reader.readexactly(size)
        size = int.from_bytes(raw, byteorder="big")

        return await self.reader.readexactly(size)
