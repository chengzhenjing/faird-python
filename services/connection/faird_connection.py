import uuid
from dataclasses import dataclass, field
from typing import List, Optional

from services.types.thread_safe_dict import ThreadSafeDict

@dataclass
class FairdConnection:
    connectionID: str
    clientIp: Optional[str]
    username: Optional[str]
    token: Optional[str]
    dataframes: ThreadSafeDict  # FlightDescriptor -> FairdDataFrame

    def __init__(self, clientIp: Optional[str] = None, username: Optional[str] = None, token: Optional[str] = None):
        self.connectionID = str(uuid.uuid4())
        self.clientIp = clientIp
        self.username = username
        self.token = token
        self.dataframes = ThreadSafeDict()

