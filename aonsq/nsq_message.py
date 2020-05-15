from dataclasses import dataclass


@dataclass
class NSQMessage:
    id: str = ""
    attempts: int = 0
    content: bytes = b""
    timestamp: int = 0
