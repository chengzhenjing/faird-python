from __future__ import annotations

from enum import Enum
from typing import Optional, List
from urllib.parse import urlparse
import pyarrow as pa
import pyarrow.flight
import json
import socket

from sdk.connection_pool import FlightConnectionPool, ConnectionManager


class DacpClient:
    def __init__(self, url: str, principal: Optional[Principal] = None):
        self.__url = url
        self.__principal = principal
        self.__token = None
        self.__connection_id = None

    @staticmethod
    def connect(url: str, principal: Optional[Principal] = None) -> DacpClient:
        client = DacpClient(url, principal)
        logger.info(f"Connecting to {url} with principal {principal}...")
        parsed = urlparse(url)
        host = f"grpc://{parsed.hostname}:{parsed.port}"
        ConnectionManager.set_connection_pool(FlightConnectionPool(host, max_connections=20))

        # 构建ticket
        try:
            client_ip = socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            client_ip = "127.0.0.1"
        ticket = {'clientIp': client_ip}
        if principal and principal.auth_type != AuthType.ANONYMOUS:
            ticket.update({
                'type': principal.params.get('type'),
                'username': principal.params.get('username'),
                'password': principal.params.get('password'),
                'controld_domain_name': principal.params.get('controld_domain_name'),
                'signature': principal.params.get('signature')
            })

        # 发送连接请求
        results = client.__connection.do_action(pa.flight.Action("connect_server", json.dumps(ticket).encode('utf-8')))
        for res in results:
            res_json = json.loads(res.body.to_pybytes().decode('utf-8'))
            client.__token = res_json.get("token")
            client.__connection_id = res_json.get("connectionID")
        return client

    def list_datasets(self) -> List[str]:
        ticket = {
            'token': self.__token,
            'page': 1,
            'limit': 999999
        }
        results = self.__connection.do_action(pa.flight.Action("list_datasets", json.dumps(ticket).encode('utf-8')))
        for res in results:
            res_json = json.loads(res.body.to_pybytes().decode('utf-8'))
            return res_json

    def get_dataset(self, dataset_name: str):
        ticket = {
            'token': self.__token,
            'dataset_name': dataset_name
        }
        results = self.__connection.do_action(pa.flight.Action("get_dataset", json.dumps(ticket).encode('utf-8')))
        for res in results:
            res_json = json.loads(res.body.to_pybytes().decode('utf-8'))
            return res_json

    def list_dataframes(self, dataset_name: str) -> List[str]:
        ticket = {
            'token': self.__token,
            'username': self.__principal.params.get('username') if self.__principal and self.__principal.params else None,
            'dataset_name': dataset_name
        }
        results = self.__connection.do_action(pa.flight.Action("list_dataframes", json.dumps(ticket).encode('utf-8')))
        for res in results:
            res_json = json.loads(res.body.to_pybytes().decode('utf-8'))
            return res_json

    def open(self, dataframe_name: str):
        from sdk.dataframe import DataFrame
        ticket = {
            'dataframe_name': dataframe_name,
            'connection_id': self.__connection_id
        }
        results = self.__connection.do_action(pa.flight.Action("open", json.dumps(ticket).encode('utf-8')))
        return DataFrame(id=dataframe_name, connection_id=self.__connection_id)

class AuthType(Enum):
    OAUTH = "oauth"
    ANONYMOUS = "anonymous"

class Principal:
    ANONYMOUS = None

    def __init__(self, auth_type: AuthType, **kwargs):
        self.auth_type = auth_type
        self.params = kwargs

    @staticmethod
    def oauth(type: str,  **kwargs) -> Principal:
        return Principal(AuthType.OAUTH, type=type, **kwargs)

    @staticmethod
    def anonymous() -> Principal:
        return Principal(AuthType.ANONYMOUS)

    def __repr__(self):
        return f"Principal(auth_type={self.auth_type}, params={self.params})"
Principal.ANONYMOUS = Principal.anonymous()


