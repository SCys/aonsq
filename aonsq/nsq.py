import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict

import aiohttp
import loguru
import orjson

from .nsq_basic import NSQBasic
from .nsq_message import NSQMessage

PKG_MAGIC = b"  V2"
RDY_SIZE = 500
MSG_SIZE = 1024 * 1024  # default is 1Mb
TSK_OVER = 0.05  # unit: second


logger = loguru.logger


@dataclass
class NSQ(NSQBasic):
    sub_mq: Dict[str, Dict[str, NSQBasic]] = field(default_factory=dict)

    async def close(self):
        await self.disconnect()

        task = self.tasks["watchdog"]
        try:
            await task
        except asyncio.CancelledError:
            pass

        for topic, channels in self.sub_mq.items():
            for channel, mq in channels.items():
                logger.debug(f"closing topic {topic} channel {channel}")
                await mq.disconnect()

                task = mq.tasks["watchdog"]
                try:
                    await task
                except asyncio.CancelledError:
                    pass

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
