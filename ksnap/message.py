"""Define message
"""
import json

from base64 import b64encode, b64decode
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple


@dataclass
class Message:

    offset: int
    key: Any
    value: Any
    timestamp: int
    headers: Optional[List[Tuple[str, bytes]]] = None

    def to_row(self):
        if self.headers:
            headers = [
                {"key": key, "val": b64encode(val).decode("ascii")}
                for key, val in self.headers
            ]
            header_str = json.dumps(headers)
        else:
            header_str = None
        return (self.offset, self.key, self.value, self.timestamp, header_str)

    @classmethod
    def from_row(cls, offset: int, key: Any, value: Any, timestamp: int,
                 header_str: Optional[str],):
        if header_str:
            headers: Optional[List[Tuple[str, bytes]]] = [
                (d["key"], b64decode(d["val"])) for d in json.loads(header_str)
            ]
        else:
            headers = None
        return cls(offset, key, value, timestamp, headers)
