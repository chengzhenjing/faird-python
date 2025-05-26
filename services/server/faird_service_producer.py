import os
import pyarrow.flight

from sdk.dataframe import DataFrame
from services.connection.faird_connection import FairdConnection
from utils.format_utils import format_arrow_table
from services.datasource.services.metacat_service import MetaCatService
from services.types.thread_safe_dict import ThreadSafeDict
from services.connection.connection_service import connect_server
from parser import *
from compute.interactive.interactive import *


class FairdServiceProducer(pa.flight.FlightServerBase):
    def __init__(self, location: pa.flight.Location):
        super(FairdServiceProducer, self).__init__(location)

        # 线程安全的字典，用于存储连接信息
        self.datasetMetas = ThreadSafeDict() # dataset_name -> DatasetMetadata
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
        connection_id = json.loads(dataframe).get("connection_id")

        # 获取 Arrow Table 的 schema
        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)
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
        connection_id = json.loads(ticket_data.get('dataframe')).get("connection_id")
        max_chunksize = ticket_data.get('max_chunksize')
        row_index = ticket_data.get('row_index')  # 获取行索引
        column_name = ticket_data.get('column_name')  # 获取列名

        # 从conn中获取dataframe.data
        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)

        if row_index is not None:  # 如果请求某行
            row_data = arrow_table.slice(row_index, 1).to_pydict()
            return pa.flight.GeneratorStream(
                pa.schema([(col, arrow_table.schema.field(col).type) for col in row_data.keys()]),
                iter([pa.RecordBatch.from_pydict(row_data)])
            )
        elif column_name is not None:  # 如果请求某列
            column_data = arrow_table[column_name].combine_chunks()
            return pa.flight.GeneratorStream(
                pa.schema([(column_name, column_data.type)]),
                iter([pa.RecordBatch.from_arrays([column_data], [column_name])])
            )
        else: # 如果没有指定行或列，则返回整个表
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
            if ticket_data.get('username'):
                token = connect_server(ticket_data.get('username'), ticket_data.get('password'))
                conn = FairdConnection(clientIp=ticket_data.get('clientIp'), username=ticket_data.get('username'), token=token)
                self.connections[conn.connectionID] = conn
                return iter([pa.flight.Result(json.dumps({"token": token, "connectionID": conn.connectionID}).encode("utf-8"))])
            elif ticket_data.get('controld_domain_name'):
                return None
            else: # 匿名访问
                conn = FairdConnection(clientIp=ticket_data.get('clientIp'))
                self.connections[conn.connectionID] = conn
                return iter([pa.flight.Result(json.dumps({"connectionID": conn.connectionID}).encode("utf-8"))])

        elif action_type == "list_datasets":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            page = int(ticket_data.get("page"))
            limit = int(ticket_data.get("limit"))
            result = pa.flight.Result(json.dumps(self.data_source_sevice.list_dataset(token=token, page=page, limit=limit)).encode())
            return iter([result])

        elif action_type == "get_dataset":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            dataset_name = ticket_data.get("dataset_name")
            meta = (self.datasetMetas.get(dataset_name)
                    or self.data_source_sevice.get_dataset_meta(token, dataset_name))
            if meta:
                self.datasetMetas[dataset_name] = meta
                return iter([pa.flight.Result(meta.model_dump_json().encode())])
            return None

        elif action_type == "list_dataframes":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            username = ticket_data.get("username")
            dataset_name = ticket_data.get("dataset_name")
            dataframe_names = self.data_source_sevice.list_dataframes(token, username, dataset_name)
            return iter([pa.flight.Result(json.dumps(dataframe_names).encode())])

        elif action_type == "open":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            dataframe_id = ticket_data.get("dataframe_id")
            connection_id = ticket_data.get('connection_id')
            # open with parser
            df = self.open_action(dataframe_id)
            # put dataframe to connection memory
            conn = self.connections.get(connection_id)
            conn.dataframes[dataframe_id] = df
            return None

        elif action_type == "to_string":
            return self.to_string_action(context, action)

        elif action_type.startswith("compute_"):
            return handle_compute_actions(self.connections, action)

        else:
            return None

    def open_action(self, dataframe_id):
        file_extension = os.path.splitext(dataframe_id)[1].lower()
        parser_switch = {
            ".csv": csv_parser.CSVParser,
            ".json": None,
            ".xml": None,
            ".nc": nc_parser.NCParser,
            ".tiff": tif_parser.TIFParser,
            ".tif": tif_parser.TIFParser,

        }
        parser_class = parser_switch.get(file_extension)
        if not parser_class:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        parser = parser_class()
        arrow_table = parser.parse(dataframe_id)
        return DataFrame(id=dataframe_id, data=arrow_table)

    def to_string_action(self, context, action):
        params = json.loads(action.body.to_pybytes().decode("utf-8"))
        dataframe_id = json.loads(params.get("dataframe")).get("id")
        actions = json.loads(params.get("dataframe")).get("actions")
        connection_id = json.loads(params.get("dataframe")).get("connection_id")
        head_rows = params.get("head_rows", 5)
        tail_rows = params.get("tail_rows", 5)
        first_cols = params.get("first_cols", 3)
        last_cols = params.get("last_cols", 3)
        display_all = params.get("display_all", False)

        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)

        table_str = format_arrow_table(arrow_table, head_rows, tail_rows, first_cols, last_cols, display_all)
        return iter([pa.flight.Result(table_str.encode("utf-8"))])
