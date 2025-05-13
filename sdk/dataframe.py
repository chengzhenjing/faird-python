import json
from typing import List, Optional, Dict, Any
import pandas
import pyarrow as pa
from tabulate import tabulate

from core.models.dataframe import DataFrame
from sdk.dacp_client import ConnectionManager


class DataFrame(DataFrame):

    def __init__(self, id: str):
        self.id = id
        self.data = None # 初始状态下 data 为空
        self.actions = [] # 用于记录操作的列表，延迟执行

    def __getitem__(self, index):
        pass

    def __str__(self) -> str:
        return self.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3)

    def collect(self) -> DataFrame:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            reader = ConnectionManager.get_connection().do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self

    def get_stream(self, max_chunksize: Optional[int] = None):
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

    def to_string(self, head_rows: int = 5, tail_rows: int = 5, first_cols: int = 3, last_cols: int = 3, display_all: bool = False) -> str:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "head_rows": head_rows,
                "tail_rows": tail_rows,
                "first_cols": first_cols,
                "last_cols": last_cols
            }
            response = ConnectionManager.get_connection().do_action(pa.flight.Action("to_string", json.dumps(ticket).encode("utf-8")))
            return response[0].body.decode("utf-8")
        else:
            if display_all:
                return self.data.to_pandas().to_string()

            all_columns = self.data.column_names
            total_columns = len(all_columns)
            total_rows = self.data.num_rows

            # 确定要显示的列
            if total_columns <= (first_cols + last_cols):
                display_columns = all_columns
            else:
                display_columns = all_columns[:first_cols] + ['...'] + all_columns[-last_cols:]

            # 确保 head_rows 和 tail_rows 不超过总行数
            head_rows = min(head_rows, total_rows)
            tail_rows = min(tail_rows, total_rows)

            # 获取头部和尾部数据
            head_data = self.data.slice(0, head_rows)
            tail_data = self.data.slice(total_rows - tail_rows, tail_rows)

            # 准备表格数据
            table_data = []

            # 添加头部数据
            for i in range(head_rows):
                row = []
                for col in display_columns:
                    if col == '...':
                        row.append('...')
                    else:
                        col_index = all_columns.index(col)
                        row.append(str(head_data.column(col_index)[i].as_py()))
                table_data.append(row)

            # 添加省略行
            if total_rows > (head_rows + tail_rows):
                table_data.append(['...' for _ in display_columns])

            # 添加尾部数据
            for i in range(tail_rows):
                row = []
                for col in display_columns:
                    if col == '...':
                        row.append('...')
                    else:
                        col_index = all_columns.index(col)
                        row.append(str(tail_data.column(col_index)[i].as_py()))
                table_data.append(row)

            # 使用 tabulate 打印
            table_str = tabulate(table_data, headers=display_columns, tablefmt="plain")
            table_str += f"\n\n[{total_rows} rows x {total_columns} columns]"
            return table_str

