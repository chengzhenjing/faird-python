# from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from services.types.thread_safe_dict import ThreadSafeDict

@dataclass
class FairdConnectionRequest:
    serverIP: str
    serverPort: int

@dataclass
class FairdConnection:
    connectionID: str
    connectionRequest: Optional[FairdConnectionRequest]
    username: Optional[str]
    token: Optional[str]
    dataframes: ThreadSafeDict  # FlightDescriptor -> FairdDataFrame


