import json

import pyarrow as pa
import pyarrow.flight

from services.connection.faird_connection import FairdConnection
from utils.format_utils import format_arrow_table
from services.datasource.services.metacat_service import MetaCatService
from services.types.thread_safe_dict import ThreadSafeDict
from services.connection.connection_service import connect_server

class FairdServiceProducer(pa.flight.FlightServerBase):
    def __init__(self, location: pa.flight.Location):
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
        dataframe = json.loads(descriptor.command.decode("utf-8")).get("dataframe")
        dataframe_id = json.loads(dataframe).get("id")
        actions = json.loads(dataframe).get("actions")

        # 获取 Arrow Table 的 schema
        arrow_table = self.get_dataframe_data(dataframe_id)
        arrow_table = self.handle_prev_actions(arrow_table, actions)
        schema = arrow_table.schema

        # 构造 FlightInfo
        ticket = pa.flight.Ticket(json.dumps({"dataframe_id": dataframe_id}).encode("utf-8"))
        endpoints = [pa.flight.FlightEndpoint(ticket, [])]
        flight_info = pa.flight.FlightInfo(
            schema,
            descriptor,
            endpoints,
            total_records=arrow_table.num_rows,
            total_bytes=arrow_table.nbytes
        )
        return flight_info

    def do_get(self, context, ticket):
        ticket_data = json.loads(ticket.ticket.decode('utf-8'))
        dataframe_id = json.loads(ticket_data.get('dataframe')).get('id')
        actions = json.loads(ticket_data.get('dataframe')).get('actions')
        max_chunksize = ticket_data.get('max_chunksize')

        # 从conn中获取dataframe.data
        arrow_table = self.get_dataframe_data(dataframe_id)
        arrow_table = self.handle_prev_actions(arrow_table, actions)

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
        if action_type == "connect_server":
            ticket_data = json.loads(action.body.to_pybytes().decode('utf-8'))
            token = connect_server(ticket_data.get('username'), ticket_data.get('password'))
            conn = FairdConnection(clientIp=ticket_data.get('clientIp'), username=ticket_data.get('username'), token=token)
            self.connections[conn.connectionID] = conn
            return iter([pa.flight.Result(json.dumps({"token": token}).encode("utf-8"))])

        elif action_type == "list_datasets":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            result = pa.flight.Result(json.dumps(self.data_source_sevice.list_dataset(token)).encode())
            return iter([result])

        elif action_type == "list_dataframes":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            username = list_req.get("username")
            dataset_id = list_req.get("dataset_id")
            dataset = (self.datasets.get(dataset_id)
                       or self.data_source_sevice.fetch_dataset_details(token, username, dataset_name))
            if dataset:
                self.datasets[dataset_name] = dataset
                return pa.flight.Result(json.dumps(dataset.dataframeIds).encode())
            else:
                return pa.flight.FlightError(f"Dataset {dataset_name} not found.")

        elif action_type == "to_string":
            return self.to_string_action(context, action)
        else:
            pass

    def to_string_action(self, context, action):
        params = json.loads(action.body.to_pybytes().decode("utf-8"))
        dataframe_id = json.loads(params.get("dataframe")).get("id")
        actions = json.loads(params.get("dataframe")).get("actions")
        head_rows = params.get("head_rows", 5)
        tail_rows = params.get("tail_rows", 5)
        first_cols = params.get("first_cols", 3)
        last_cols = params.get("last_cols", 3)
        display_all = params.get("display_all", False)

        arrow_table = self.get_dataframe_data(dataframe_id)
        arrow_table = self.handle_prev_actions(arrow_table, actions)

        table_str = format_arrow_table(arrow_table, head_rows, tail_rows, first_cols, last_cols, display_all)
        return iter([pa.flight.Result(table_str.encode("utf-8"))])

    def get_dataframe_data(self, dataframe_id: str) -> pa.Table:
        import pandas as pd
        df = pd.read_csv(dataframe_id)
        return pa.Table.from_pandas(df)

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

