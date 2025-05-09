from urllib.parse import urlparse, ParseResult
from typing import Optional

class Connection:
    def __init__(
        self,
        protocol: str = None,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        url: str = None
    ):
        """
        支持两种初始化方式：
        1. 直接参数：protocol, host, port, username, password
        2. URL 字符串：dacp://admin:admin@10.0.82.71:3101
        """
        if url:
            self._init_from_url(url)
        else:
            self.protocol = protocol
            self.host = host
            self.port = port
            self.username = username
            self.password = password

        # 验证必要参数
        if not all([self.protocol, self.host, self.port]):
            raise ValueError("Missing required connection parameters")

    def _init_from_url(self, url: str):
        """从 URL 解析连接参数"""
        parsed: ParseResult = urlparse(url)
        self.protocol = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port
        if parsed.username:
            self.username = parsed.username
            self.password = parsed.password

    def __enter__(self):
        """with 语句支持"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """with 语句结束时自动关闭"""
        self.close()

    def connect(self):
        """实际连接逻辑"""
        print(f"Connecting to {self.protocol}://{self.host}:{self.port}...")
        # 实现真实连接代码
        return self

    def close(self):
        """关闭连接"""
        print("Connection closed.")

    def __repr__(self):
        return f"Connection(protocol='{self.protocol}', host='{self.host}', port={self.port})"