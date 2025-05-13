import json
import pyarrow.flight as flight

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
        # 实现数据检索逻辑
        pass

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

        return None
