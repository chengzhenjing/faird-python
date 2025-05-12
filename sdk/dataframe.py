import json
from typing import List, Optional, Dict, Any
import pandas
import pyarrow as pa
from core.models.dataframe import DataFrame
from sdk.dacp_client import ConnectionManager


class DataFrame(DataFrame):

    def __init__(self, id: str):
        self.id = id
        self.data = None # 初始状态下 data 为空
        self.actions = [] # 用于记录操作的列表

    def __getitem__(self, index):
        pass

    def __str__(self) -> str:
        pass

    def collect(self) -> DataFrame:
        if self.data is None:
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self


    def get_stream(self, max_chunksize: Optional[int] = None) -> List[pa.RecordBatch]:
        if self.data is None:
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self.data.to_batches(max_chunksize)

    def limit(self, rowNum: int) -> DataFrame:
        self.actions.append(("limit", {"rowNum": rowNum}))
        return self

    def slice(self, offset: int = 0, length: Optional[int] = None) -> DataFrame:
        self.actions.append(("slice", {"offset": offset, "length": length}))
        return self

    def select(self, *columns):
        self.actions.append(("select", {"columns": columns}))
        return self

    def filter(self, mask: pa.Array) -> DataFrame:
        self.actions.append(("filter", {"mask": mask}))
        return self

    def sum(self, column: str):
        pass

    def map(self, column: str, func: Any, new_column_name: Optional[str] = None) -> DataFrame:
        self.actions.append(("map", {"column": column, "func": func, "new_column_name": new_column_name}))
        return self

    def to_pandas(self, **kwargs) -> pandas.DataFrame:
        if self.data is None:
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self.data.to_pandas(**kwargs)

    def to_pydict(self) -> Dict[str, List[Any]]:
        if self.data is None:
            reader = ConnectionManager.get_connection().do_get(
                pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self.data.to_pydict()

    def to_string(self, head_rows: int = 5, tail_rows: int = 5, first_cols: int = 3, last_cols: int = 3) -> str:
        pass
