from __future__ import annotations
from typing import Optional, List
from enum import Enum
from urllib.parse import urlparse
import pyarrow as pa
import pyarrow.flight
import json

class DacpClient:
    def __init__(self, url: str, principal: Principal):
        self.url = url
        self.principal = principal
        self.connection = None
        self.token = None

    @staticmethod
    def connect(url: str, principal: Principal) -> DacpClient:
        client = DacpClient(url, principal)
        print(f"Connecting to {url} with principal {principal}...")
        parsed = urlparse(url)
        host = f"grpc://{parsed.hostname}:{parsed.port}"
        client.connection =  pa.flight.connect(host)
        ConnectionManager.set_connection(client.connection)
        connect_request = {
            'principal': principal
        }
        results = client.connection.do_action(pa.flight.Action("connect_server", json.dumps(connect_request).encode('utf-8')))
        for res in results:
            res_json: dict = json.loads(res.body.to_pybytes().decode('utf-8'))
            client.token = res_json.get("token")
        return client

    def list_datasets(self) -> List[str]:
        results = self.connection.do_action(pa.flight.Action("list_datasets", self.token.encode('utf-8')))
        return ["dataset1", "dataset2", "dataset3"]

    def list_dataframes(self, dataset_name: str) -> List[str]:
        results = self.connection.do_action(pa.flight.Action("list_dataframes", self.token.encode('utf-8')))
        return ["/Users/yaxuan/Documents/2024/工作/faird/2024-03-快速发版/测试文件/common/2019年中国榆林市沟道信息.csv", "dataframe2"]

    def open(self, dataframe_id: str):
        from sdk.dataframe import DataFrame
        results = self.connection.do_action(pa.flight.Action("open", self.token.encode('utf-8')))
        for res in results:
            res_json: dict = json.loads(res.body.to_pybytes().decode('utf-8'))
            dataframe = res_json.get("dataframe_id")
        return DataFrame(id=dataframe_id)

class AuthType(Enum):
    OAUTH = "oauth"
    ANONYMOUS = "anonymous"

class Principal:
    def __init__(self, auth_type: AuthType, **kwargs):
        self.auth_type = auth_type
        self.params = kwargs

    @staticmethod
    def oauth(type: str, username: str, password: str) -> Principal:
        return Principal(AuthType.OAUTH, type=type, username=username, password=password)

    @staticmethod
    def anonymous() -> Principal:
        return Principal(AuthType.ANONYMOUS)

    def __repr__(self):
        return f"Principal(auth_type={self.auth_type}, params={self.params})"

class ConnectionManager:
    _connection: Optional[pyarrow.flight.FlightClient] = None

    @staticmethod
    def set_connection(connection: pyarrow.flight.FlightClient):
        ConnectionManager._connection = connection

    @staticmethod
    def get_connection() -> pyarrow.flight.FlightClient:
        if ConnectionManager._connection is None:
            raise RuntimeError("Connection has not been initialized.")
        return ConnectionManager._connection