import json
from typing import List, Optional, Dict, Any

import duckdb
import pandas
import pyarrow as pa
import pyarrow.compute as pc

from core.models.dataframe import DataFrame
from sdk.dacp_client import ConnectionManager
from utils.format_utils import format_arrow_table


class DataFrame(DataFrame):

    def __init__(self, id: str, data: Optional[pa.Table] = None, actions: Optional[List[tuple]] = [], connection_id: Optional[str] = None):
        self.id = id
        self.data = data # 初始状态下 data 为空
        self.actions = actions # 用于记录操作的列表，延迟执行
        self.connection_id = connection_id

    def __str__(self) -> str:
        return self.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3)

    def __len__(self) -> int:
        return self.num_rows

    def __getitem__(self, index):
        if isinstance(index, int):  # 行选择
            if self.data is None:
                ticket = {
                    "dataframe": json.dumps(self, default=vars),
                    "row_index": index
                }
                reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
                row_data = reader.read_all().to_pydict()
                return {col: row_data[col][0] for col in row_data}
            return {col: self.data[col][index].as_py() for col in self.data.column_names}
        elif isinstance(index, str):  # 列选择
            if self.data is None:
                ticket = {
                    "dataframe": json.dumps(self, default=vars),
                    "column_name": index
                }
                reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
                column_data = reader.read_all()
                return column_data.column(0).combine_chunks().to_pylist()  # 转换为列表
            return self.data[index].combine_chunks().to_pylist()
        else:
            raise TypeError("Index must be an integer (row) or string (column).")


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
    def num_rows(self) -> int:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return flight_info.total_records
        return self.data.num_rows

    @property
    def num_cols(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return len(flight_info.schema)
        return len(self.data.column_names)

    @property
    def shape(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            num_rows = flight_info.total_records
            num_cols = len(flight_info.schema)
            return (num_rows, num_cols)
        return self.data.shape

    @property
    def column_names(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return flight_info.schema.names
        return self.data.column_names

    @property
    def total_bytes(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            flight_info = ConnectionManager.get_connection().get_flight_info(descriptor)
            return flight_info.total_bytes
        return self.data.nbytes

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

    def filter(self, expression: str) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("filter", {"expression": expression}))
        return new_df

    def sum(self, column: str):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            results = ConnectionManager.get_connection().do_action(
                pa.flight.Action("compute_sum", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.sum(arrow_table[column]).as_py()

    def mean(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            results = ConnectionManager.get_connection().do_action(
                pa.flight.Action("compute_mean", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.mean(arrow_table[column]).as_py()

    def min(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            results = ConnectionManager.get_connection().do_action(
                pa.flight.Action("compute_min", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.min(arrow_table[column]).as_py()

    def max(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            results = ConnectionManager.get_connection().do_action(
                pa.flight.Action("compute_max", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.max(arrow_table[column]).as_py()

    def sort(self, column: str, order: str = "ascending") -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("sort", {"column": column, "order": order}))
        return new_df

    def sql(self, sql_str: str) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("sql", {"sql_str": sql_str}))
        return new_df

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

    # todo: 补充
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
                column_names = arrow_table.column_names
                locals_dict = {col: arrow_table[col].to_pandas() for col in column_names}
                mask = eval(params.get("expression"), {"__builtins__": None}, locals_dict)
                arrow_table = arrow_table.filter(pa.array(mask))
            elif action_type == "map":
                column = params.get("column")
                func = params.get("func")
                new_column_name = params.get("new_column_name", f"{column}_mapped")
                column_data = arrow_table[column].to_pylist()
                mapped_data = [func(value) for value in column_data]
                arrow_table = arrow_table.append_column(new_column_name, pa.array(mapped_data))
            elif action_type == "sort":
                column = params.get("column")
                order = params.get("order", "ascending")
                if order == "ascending":
                    arrow_table = arrow_table.sort_by(column)
                else:
                    arrow_table = arrow_table.sort_by([(column, "descending")])
            elif action_type == "sql":
                dataframe = arrow_table
                arrow_table = duckdb.sql(params.get("sql_str")).arrow()
            else:
                raise ValueError(f"Unsupported action type: {action_type}")
        return arrow_table