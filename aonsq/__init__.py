import asyncio
import random
import string
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp
import orjson
from loguru import logger

i = logger.info
d = logger.debug
w = logger.warning
e = logger.error

PKG_MAGIC = b"  V2"
RDY_SIZE = 1000
MSG_SIZE = 1024 * 1024  # default is 1Mb


async def public_ip():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.ip.sb/ip") as response:
            return await response.text()


@dataclass
class NSQMessage:
    id: str = ""
    attempts: int = 0
    content: bytes = b""
    timestamp: int = 0


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
        reader, writer = await asyncio.open_connection(self.host, self.port)

        writer.write(b"  V2")
        await writer.drain()

        self.reader = reader
        self.writer = writer

        self.is_connect = True

        # d(f"nsq:{self.host}:{self.port}")

        if not await self.send_identify():
            w("send identify error")
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

            await self.writer.wait_closed()

            self.writer = None
            self.reader = None

        self.is_connect = False

    async def send_identify(self):
        info = orjson.dumps(
            {
                "hostname": await public_ip(),
                "client_id": "".join(random.choice(string.ascii_lowercase) for _ in range(8)),
                "user_agent": "aonsq.py/0.0.3",
                "deflate": True,
                "deflate_level": 5,
            }
        )

        self.writer.write(f"IDENTIFY\n".encode() + len(info).to_bytes(4, "big") + info)
        await self.writer.drain()

        raw = await self.reader.readexactly(4)
        size = int.from_bytes(raw, byteorder="big")

        resp = await self.reader.readexactly(size)
        if resp[4:] == b"OK":
            return True

        w(f"identify error:{resp[4:].decode()}")
        return False

    async def send_sub(self):
        self.writer.write(f"SUB {self.topic} {self.channel}\n".encode())
        await self.writer.drain()

    async def send_rdy(self):
        self.writer.write(f"RDY {self.rdy}\n".encode())
        await self.writer.drain()

    async def _tx_worker(self):
        self._busy_tx = True

        while self.is_connect:
            # break the main loop
            if self._connect_is_broken:
                break

            tx_size = self.tx_queue.qsize()
            if tx_size == 0:
                await asyncio.sleep(0.1)
                continue

            messages = {}
            queues = []
            for _ in range(tx_size):
                topic, content = await self.tx_queue.get()
                self.tx_queue.task_done()

                if topic not in messages:
                    messages[topic] = [0, b""]

                messages[topic][0] += 1
                messages[topic][1] += len(content).to_bytes(4, "big") + content
                queues.append((topic, content))

            for topic, content in messages.items():
                raw = f"MPUB {topic}\n".encode()
                raw += len(content[1]).to_bytes(4, "big")
                raw += content[0].to_bytes(4, "big")
                raw += content[1]

                queues.pop()

                try:
                    self.writer.write(raw)
                    await self.writer.drain()

                    self.sent += content[0]

                except ConnectionError as exc:
                    w(f"connection error, recovery the unsent messages:{str(exc)}")

                    # recovery in the tx_queue end
                    for item in queues:
                        self.tx_queue.put_nowait(item)

                    self._connect_is_broken = True

                    break

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
                e(f"read head connection error:{str(exc)}")

                self._connect_is_broken = True
                break

            except asyncio.exceptions.IncompleteReadError as exc:
                e(f"steam incomplete read error:{str(exc)}")

                self._connect_is_broken = True
                break

            size = int.from_bytes(raw, byteorder="big")
            if MSG_SIZE < size or size < 6:
                w(f"invalid size:{size} {raw}")
                continue

            try:
                resp = await self.reader.readexactly(size)
            except ConnectionError as exc:
                e(f"read content connection error:{str(exc)}")

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
                    self.writer.write(b"NOP\n")
                    continue

                d(f"response /{frame_data.decode()}/")
                continue

            elif frame_type == 1:  # error
                w(f"error response:{frame_data.decode()}")

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
                    d(f"frame {size} {frame_type} /{frame_data}/")

                continue

            d(f"frame {size} {frame_type} /{frame_data}/")

        self._busy_rx = False

    async def _sub_worker(self):
        self._busy_sub = True

        while self.is_connect:
            msg = await self.rx_queue.get()

            if self.rdy <= 0:
                # d(f"sub {self.topic} {self.channel} cost {self.cost}")
                self.rdy = RDY_SIZE

                while True:
                    try:
                        self.writer.write(f"RDY {self.rdy}\n".encode())
                        await self.writer.drain()
                        break
                    except ConnectionError as e:
                        w(f"rdy with connection error:{e}")
                        self.reader.set_exception(e)
                        await asyncio.sleep(3)

            self.rdy -= 1
            send_fin = True
            if self.handler is not None:
                send_fin = await self.handler(msg)

            try:
                if send_fin:
                    self.writer.write(f"FIN {msg.id}\n".encode())
                else:
                    self.writer.write(f"REQ {msg.id}\n".encode())

                await self.writer.drain()
            except ConnectionError as e:
                w(f"fin/req with connection error")
                self.reader.set_exception(e)

                self._connect_is_broken = True
                break

            self.rx_queue.task_done()

        self._busy_sub = False

    async def _watchdog(self):
        self._busy_sub = True

        # stauts is recover or normal
        while self.is_connect:
            await asyncio.sleep(1)

            if self._connect_is_broken:
                await asyncio.sleep(5)

                d("nsq connection is being reconnected")

                await self.disconnect()
                await asyncio.sleep(1)

                await self.connect()

                await self.send_sub()
                await self.send_rdy()

        self._busy_watchdog = False


@dataclass
class NSQ(NSQBasic):
    sub_mq: Dict[str, Dict[str, NSQBasic]] = field(default_factory=dict)

    async def disconnect(self):
        await super().disconnect()

        for topic, channels in self.sub_mq.items():
            for channel, mq in channels.items():
                d(f"closing topic {topic} channel {channel}")
                await mq.disconnect()

    async def pub(self, topic: str, data: bytes):
        if not self.is_connect:
            w("connection invalid status")
            return False

        await self.tx_queue.put((topic, data))
        # d(f"pub {topic} {self.tx_queue.qsize()}")
        return True

    async def sub(self, topic: str, channel: str, handler: Callable[[NSQMessage], Awaitable[bool]]):
        if topic not in self.sub_mq:
            self.sub_mq[topic] = {}

        if channel in self.sub_mq:
            return self.sub_mq[topic][channel]

        cli = NSQBasic(host=self.host, port=self.port, topic=topic, channel=channel, handler=handler)
        await cli.connect()
        if not cli.is_connect:
            return

        await cli.send_sub()
        await cli.send_rdy()

        self.sub_mq[topic][channel] = cli

        return cli


if __name__ == "__main__":

    async def msg_handler(msg: NSQMessage) -> bool:
        # d(f"msg: {msg.id}")
        return True

    async def test():
        mq = NSQ(host="127.0.0.1", port=4070)
        await mq.connect()
        await mq.sub("demo", "test", msg_handler)

        while True:
            for j in range(1000):
                await mq.pub("demo", orjson.dumps({"id": j, "ts_created": datetime.now(timezone.utc)}))

            await asyncio.sleep(1)

    try:
        asyncio.get_event_loop().run_until_complete(test())
    except KeyboardInterrupt:
        pass
