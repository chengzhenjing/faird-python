from __future__ import annotations

import pandas
import pyarrow as pa
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any


class DataFrame(ABC):

    def __init__(self, id: str):
        self.id = id
        self.data = None
        self.actions = []

    @abstractmethod
    def __getitem__(self, index):
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def collect(self) -> DataFrame:
        pass

    @abstractmethod
    def get_stream(self, max_chunksize: Optional[int] = None):
        pass

    @abstractmethod
    def limit(self, rowNum: int) -> DataFrame:
        pass

    @abstractmethod
    def slice(self, offset: int = 0, length: Optional[int] = None) -> DataFrame:
        pass

    @abstractmethod
    def select(self, *columns):
        pass

    @abstractmethod
    def filter(self, mask: pa.Array) -> DataFrame:
        pass

    @abstractmethod
    def sum(self, column: str):
        pass

    @abstractmethod
    def map(self, column: str, func: Any, new_column_name: Optional[str] = None) -> DataFrame:
        pass

    @abstractmethod
    def to_pandas(self, **kwargs) -> pandas.DataFrame:
        pass

    @abstractmethod
    def to_pydict(self) -> Dict[str, List[Any]]:
        pass

    @abstractmethod
    def to_string(
            self,
            head_rows: int = 5,
            tail_rows: int = 5,
            first_cols: int = 3,
            last_cols: int = 3,
            display_all: bool = False
    ) -> str:
        pass

    @staticmethod
    def from_pandas(pdf: pandas.DataFrame, schema=None) -> DataFrame:
        pass