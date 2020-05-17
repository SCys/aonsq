import asyncio
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import aiohttp
import loguru
import orjson

from .nsq_interface import NSQInterface
from .nsq_message import NSQMessage

logger = loguru.logger

PKG_MAGIC = b"  V2"
RDY_SIZE = 500
MSG_SIZE = 1024 * 1024  # default is 1Mb
TSK_OVER = 0.025  # unit: second


@dataclass
class NSQBasic(NSQInterface):
    topic: str = ""
    channel: str = ""

    rx_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=RDY_SIZE * 20))
    tx_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=RDY_SIZE * 50))
    handler: Optional[Callable[[Any], Awaitable[bool]]] = None

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
                if self.topic and self.channel:
                    logger.error(f"topic {self.topic}/{self.channel} read error")

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

                task = asyncio.create_task(self._sub_worker_async_task(self.handler, msg))
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

            try:
                await self.disconnect()
            except:
                if self.topic and self.channel:
                    logger.exception(f"topic {self.topic}/{self.channel} disconnect error")
                else:
                    logger.exception(f"topic disconnect error")

            while not self.is_connect:
                try:
                    await self.connect()

                    if self.topic and self.channel:
                        await self.send_sub()
                        await self.send_rdy()

                    break  # once

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

            if self.topic and self.channel:
                logger.debug(f"topic {self.topic}/{self.channel} reconnected")
            else:
                logger.debug(f"reconnected")

    async def _sub_worker_async_task(self, handler, msg):
        try:
            result = await self.handler(msg)
        except asyncio.TimeoutError as e:
            logger.error(f"topic {self.topic}/{self.channel} handler error:{e}")

            result = False

        try:
            if result:
                await self.write(f"FIN {msg.id}\n")
            else:
                await self.write(f"REQ {msg.id}\n")

        except ConnectionError as e:
            logger.warning(f"topic {self.topic}/{self.channel} fin/req with connection error")

            self._connect_is_broken = True

        self.rdy -= 1
