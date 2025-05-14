import json
from typing import List, Optional, Dict, Any
import pandas
import pyarrow as pa

from core.models.dataframe import DataFrame
from sdk.dacp_client import ConnectionManager
from utils.format_utils import format_arrow_table


class DataFrame(DataFrame):

    def __init__(self, id: str, data: Optional[pa.Table] = None, actions: Optional[List[tuple]] = [], connection_id: Optional[str] = None):
        self.id = id
        self.data = data # 初始状态下 data 为空
        self.actions = actions # 用于记录操作的列表，延迟执行
        self.connection_id = connection_id

    def __getitem__(self, index):
        pass

    def __str__(self) -> str:
        return self.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3)

    @property
    def schema(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return flight_info.schema
        return self.data.schema

    @property
    def num_rows(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return flight_info.total_records
        return self.data.num_rows

    def collect(self) -> DataFrame:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self

    def get_stream(self, max_chunksize: Optional[int] = 1000):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "max_chunksize": max_chunksize
            }
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            for batch in reader:
                yield batch.data
            self.actions = []
        else:
            for batch in self.data.to_batches(max_chunksize):
                yield batch

    def limit(self, rowNum: int) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("limit", {"rowNum": rowNum}))
        return new_df

    def slice(self, offset: int = 0, length: Optional[int] = None) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("slice", {"offset": offset, "length": length}))
        return new_df

    def select(self, *columns):
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("select", {"columns": columns}))
        return new_df

    def filter(self, mask: pa.Array) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("filter", {"mask": mask}))
        return new_df

    def sum(self, column: str):
        pass

    def map(self, column: str, func: Any, new_column_name: Optional[str] = None) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("map", {"column": column, "func": func, "new_column_name": new_column_name}))
        return new_df

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

    def to_string(self, head_rows: int = 5, tail_rows: int = 5, first_cols: int = 3, last_cols: int = 3, display_all: bool = False) -> str:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "head_rows": head_rows,
                "tail_rows": tail_rows,
                "first_cols": first_cols,
                "last_cols": last_cols
            }
            results = ConnectionManager.get_connection().do_action(pa.flight.Action("to_string", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return res.body.to_pybytes().decode('utf-8')
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return format_arrow_table(arrow_table, head_rows, tail_rows, first_cols, last_cols, display_all)

    def handle_prev_actions(self, arrow_table, prev_actions):
        for action in prev_actions:
            action_type, params = action
            if action_type == "limit":
                row_num = params.get("rowNum")
                arrow_table = arrow_table.slice(0, row_num)
            elif action_type == "slice":
                offset = params.get("offset", 0)
                length = params.get("length")
                arrow_table = arrow_table.slice(offset, length)
            elif action_type == "select":
                columns = params.get("columns")
                arrow_table = arrow_table.select(columns)
            elif action_type == "filter":
                mask = params.get("mask")
                arrow_table = arrow_table.filter(mask)
            elif action_type == "map":
                column = params.get("column")
                func = params.get("func")
                new_column_name = params.get("new_column_name", f"{column}_mapped")
                column_data = arrow_table[column].to_pylist()
                mapped_data = [func(value) for value in column_data]
                arrow_table = arrow_table.append_column(new_column_name, pa.array(mapped_data))
            else:
                raise ValueError(f"Unsupported action type: {action_type}")
        return arrow_table