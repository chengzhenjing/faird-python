import json

import pyarrow as pa
import pyarrow.flight as flight
from tabulate import tabulate


from services.datasource.services.metacat_service import MetaCatService
from services.types.thread_safe_dict import ThreadSafeDict

class FairdServiceProducer(flight.FlightServerBase):
    def __init__(self, location: flight.Location):
        super(FairdServiceProducer, self).__init__(location)

        # 线程安全的字典，用于存储连接信息
        self.datasets = ThreadSafeDict() # dataset_name -> Dataset
        self.connections = ThreadSafeDict() # connection_id -> Connection
        self.user_compute_resources = ThreadSafeDict()  # username -> UserComputeResource

        # 初始化datasource_service todo: 根据传入service_class_name动态加载
        self.data_source_sevice = MetaCatService()

    def list_flights(self, context, criteria):
        # 实现列出可用的 Flight 数据集
        pass

    def get_flight_info(self, context, descriptor):
        # 返回特定 Flight 的元数据
        pass

    def do_get(self, context, ticket):
        ticket_data = json.loads(ticket.ticket.decode('utf-8'))
        dataframe_id = json.loads(ticket_data.get('dataframe')).get('id')
        max_chunksize = ticket_data.get('max_chunksize')

        # 从conn中获取dataframe.data
        arrow_table = self.get_dataframe_data(dataframe_id)

        # todo: handle previous actions

        if max_chunksize:
            batches = arrow_table.to_batches(max_chunksize)
        else:
            batches = arrow_table.to_batches()
        return pa.flight.GeneratorStream(arrow_table.schema, iter(batches))

    def do_put(self, context, descriptor, reader, writer):
        # 实现数据写入逻辑
        pass

    def do_action(self, context, action):
        action_type = action.type
        # 处理不同的操作
        if action_type == "list_datasets":
            list_req = json.loads(action.body.decode())
            token = list_req.get("token")
            return flight.Result(json.dumps(self.data_source_sevice.list_dataset(token)).encode())
        elif action_type == "list_dataframes":
            list_req = json.loads(action.body.decode())
            token = list_req.get("token")
            username = list_req.get("username")
            dataset_name = list_req.get("dataset_name")
            dataset = (self.datasets.get(dataset_name)
                       or self.data_source_sevice.fetch_dataset_details(token, username, dataset_name))
            if dataset:
                self.datasets[dataset_name] = dataset
                return flight.Result(json.dumps(dataset.dataframeIds).encode())
            else:
                return flight.FlightError(f"Dataset {dataset_name} not found.")
        elif action_type == "to_string":
            return self.to_string_action(context, action)
        else:
            pass

    def to_string_action(self, context, action):
        params = json.loads(action.body.to_pybytes().decode("utf-8"))
        dataframe_id = json.loads(params.get("dataframe")).get("id")
        head_rows = params.get("head_rows", 5)
        tail_rows = params.get("tail_rows", 5)
        first_cols = params.get("first_cols", 3)
        last_cols = params.get("last_cols", 3)
        display_all = params.get("display_all", False)

        arrow_table = self.get_dataframe_data(dataframe_id)
        if display_all:
            return arrow_table.to_pandas().to_string()
        all_columns = arrow_table.column_names
        total_columns = len(all_columns)
        total_rows = arrow_table.num_rows

        # 确定要显示的列
        if total_columns <= (first_cols + last_cols):
            display_columns = all_columns
        else:
            display_columns = all_columns[:first_cols] + ['...'] + all_columns[-last_cols:]

        # 确保 head_rows 和 tail_rows 不超过总行数
        head_rows = min(head_rows, total_rows)
        tail_rows = min(tail_rows, total_rows)

        # 获取头部和尾部数据
        head_data = arrow_table.slice(0, head_rows)
        tail_data = arrow_table.slice(total_rows - tail_rows, tail_rows)

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
        return [flight.Result(table_str.encode("utf-8"))]

    def get_dataframe_data(self, dataframe_id: str) -> pa.Table:
        import pandas as pd
        df = pd.read_csv(dataframe_id)
        return pa.Table.from_pandas(df)
