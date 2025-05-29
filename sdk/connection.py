# faird/sdk/flight_client.py

import pyarrow.flight as flight


class FlightConnection:
    def __init__(self, host: str = "localhost", port: int = 8815):
        """
        初始化与远程 PyArrow Flight 服务的连接

        Args:
            host (str): 服务器地址
            port (int): 服务端口
        """
        self.host = host
        self.port = port
        self.client = flight.connect((host, port))

    def get_table(self, dataframe_id: str):
        """
        获取远程 Arrow Table

        Args:
            dataframe_id (str): 唯一标识符（文件名或 ID）

        Returns:
            pa.Table: 返回从远程读取的 Arrow 表格
        """
        ticket = flight.Ticket(dataframe_id.encode('utf-8'))
        reader = self.client.do_get(ticket)
        return reader.read_all()

    def write_table(self, table, dataframe_id: str):
        """
        将 Arrow Table 写入远程服务

        Args:
            table (pa.Table): 要上传的 Arrow 表格
            dataframe_id (str): 远程标识符
        """
        descriptor = flight.FlightDescriptor.for_path(dataframe_id)
        writer, _ = self.client.do_put(descriptor, table.schema)
        writer.write_table(table)
        writer.close()

    def get_schema(self, dataframe_id: str):
        """
        获取远程数据集的 schema

        Args:
            dataframe_id (str): 标识符

        Returns:
            Schema: Arrow schema 对象
        """
        descriptor = flight.FlightDescriptor.for_path(dataframe_id)
        info = self.client.get_flight_info(descriptor)
        return info.schema

    def list_datasets(self, criteria=None):
        """
        列出所有可用的数据集

        Args:
            criteria: 查询条件（可选）
        """
        flights = self.client.list_flights(criteria)
        for info in flights:
            logger.info(f"Dataset: {info.descriptor.path[0]}, Rows: {info.total_records}, Size: {info.total_bytes}")

    def do_action(self, action_type: str, body: bytes = b''):
        """
        执行一个远程操作（Action）

        Args:
            action_type (str): 操作类型（如 refresh_cache）
            body (bytes): 操作内容（可选）

        Returns:
            List[Any]: 操作结果列表
        """
        action = flight.Action(type=action_type, body=body)
        results = self.client.do_action(action)
        return [result.body.to_pybytes() for result in results]

    def close(self):
        """关闭连接"""
        self.client.close()
