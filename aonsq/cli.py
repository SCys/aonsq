import sys
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict

from loguru import logger as root_logger

from .cli_basic import NSQBasic
from .message import NSQMessage

root_logger.add(
    sys.stderr,
    format="[{level} {time} {file}:{function}:{line}]{message}",
    filter=lambda r: "aonsq" in r["extra"],
    level="INFO",
)

logger = root_logger.bind(aonsq=True)

i = logger.info
d = logger.debug
w = logger.warning
e = logger.error
x = logger.exception


@dataclass
class NSQ(NSQBasic):
    # subscription listeners
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
