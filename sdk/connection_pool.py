import atexit
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Optional, Dict, Any
import pyarrow.flight as fl
import pyarrow as pa
from pyarrow._flight import FlightClient


class FlightConnectionPool:
    """Arrow Flight RPC 连接池实现 - 改进版"""

    def __init__(self,
                 location: str,
                 max_connections: int = 10,
                 min_connections: int = 1,
                 connection_timeout: int = 30,
                 idle_timeout: int = 300,
                 max_wait_time: int = 60,  # 新增：最大等待时间
                 enable_blocking_wait: bool = True,  # 新增：是否启用阻塞等待
                 **connection_kwargs):
        """
        初始化连接池

        Args:
            location: Flight 服务器地址
            max_connections: 最大连接数
            min_connections: 最小连接数
            connection_timeout: 连接超时时间(秒)
            idle_timeout: 连接空闲超时时间(秒)
            max_wait_time: 等待可用连接的最大时间(秒)
            enable_blocking_wait: 是否启用阻塞等待模式
            **connection_kwargs: 传递给 FlightClient 的其他参数
        """
        self.location = location
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        self.max_wait_time = max_wait_time
        self.enable_blocking_wait = enable_blocking_wait
        self.connection_kwargs = connection_kwargs

        # 连接池队列
        self._pool = queue.Queue(maxsize=max_connections)
        self._active_connections = {}  # 活跃连接
        self._connection_count = 0
        self._lock = threading.RLock()

        # 新增：等待队列，用于管理等待连接的请求
        self._waiting_queue = queue.Queue()
        self._shutdown = False

        # 初始化最小连接数
        self._initialize_pool()

        # 启动清理线程
        self._cleanup_thread = threading.Thread(target=self._cleanup_idle_connections, daemon=True)
        self._cleanup_thread.start()

    def _create_connection(self) -> fl.FlightClient:
        """创建新的 Flight 连接"""
        try:
            client = fl.connect(self.location, **self.connection_kwargs)
            return client
        except Exception as e:
            raise ConnectionError(f"Failed to create Flight connection: {e}")

    def _initialize_pool(self):
        """初始化连接池"""
        with self._lock:
            for _ in range(self.min_connections):
                try:
                    conn = self._create_connection()
                    self._pool.put({
                        'client': conn,
                        'created_at': time.time(),
                        'last_used': time.time()
                    })
                    self._connection_count += 1
                except Exception as e:
                    print(f"Warning: Failed to initialize connection: {e}")

    def _cleanup_idle_connections(self):
        """清理空闲连接的后台线程"""
        while not self._shutdown:
            try:
                time.sleep(60)  # 每分钟检查一次
                current_time = time.time()

                with self._lock:
                    # 收集需要清理的连接
                    temp_connections = []

                    while not self._pool.empty() and self._connection_count > self.min_connections:
                        try:
                            conn_info = self._pool.get_nowait()
                            if current_time - conn_info['last_used'] > self.idle_timeout:
                                # 连接空闲时间过长，关闭连接
                                try:
                                    conn_info['client'].close()
                                except:
                                    pass
                                self._connection_count -= 1
                            else:
                                # 连接仍然有效，放回队列
                                temp_connections.append(conn_info)
                        except queue.Empty:
                            break

                    # 将有效连接放回队列
                    for conn_info in temp_connections:
                        self._pool.put(conn_info)

            except Exception as e:
                if not self._shutdown:
                    print(f"Error in cleanup thread: {e}")

    def _get_connection(self, timeout: Optional[float] = None) -> fl.FlightClient:
        """
        从连接池获取连接 - 改进版

        Args:
            timeout: 获取连接的超时时间

        Returns:
            FlightClient 实例
        """
        if timeout is None:
            timeout = self.connection_timeout

        start_time = time.time()

        while True:
            try:
                # 尝试从池中获取现有连接
                conn_info = self._pool.get(timeout=0.1)  # 短时间超时
                conn_info['last_used'] = time.time()

                # 将连接标记为活跃
                conn_id = id(conn_info['client'])
                with self._lock:
                    self._active_connections[conn_id] = conn_info

                return conn_info['client']

            except queue.Empty:
                # 检查是否可以创建新连接
                with self._lock:
                    if self._connection_count < self.max_connections:
                        try:
                            client = self._create_connection()
                            self._connection_count += 1

                            conn_info = {
                                'client': client,
                                'created_at': time.time(),
                                'last_used': time.time()
                            }

                            conn_id = id(client)
                            self._active_connections[conn_id] = conn_info

                            return client
                        except Exception as e:
                            print(f"Failed to create new connection: {e}")

                # 如果启用阻塞等待且未超时，继续等待
                if self.enable_blocking_wait:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        raise ConnectionError(f"Timeout waiting for available connection after {elapsed:.2f}s")

                    # 短暂睡眠后重试
                    time.sleep(0.1)
                else:
                    # 不启用阻塞等待，直接抛出异常
                    raise ConnectionError("Connection pool exhausted and max connections reached")

    def return_connection(self, client: fl.FlightClient):
        """
        将连接返回到连接池

        Args:
            client: 要返回的 FlightClient 实例
        """
        conn_id = id(client)

        with self._lock:
            if conn_id in self._active_connections:
                conn_info = self._active_connections.pop(conn_id)
                conn_info['last_used'] = time.time()

                try:
                    # 检查连接是否仍然有效
                    # 这里可以添加连接健康检查
                    self._pool.put(conn_info, block=False)
                except queue.Full:
                    # 连接池已满，关闭连接
                    try:
                        client.close()
                    except:
                        pass
                    self._connection_count -= 1

    @contextmanager
    def get_client(self, timeout: Optional[float] = None):
        """
        上下文管理器方式获取连接

        Usage:
            with pool.get_client() as client:
                # 使用 client 进行操作
                pass
        """
        client = None
        try:
            client = self._get_connection(timeout=timeout)
            yield client
        finally:
            if client:
                self.return_connection(client)

    def close_all(self):
        """关闭所有连接"""
        self._shutdown = True

        with self._lock:
            # 关闭活跃连接
            for conn_info in self._active_connections.values():
                try:
                    conn_info['client'].close()
                except:
                    pass
            self._active_connections.clear()

            # 关闭池中的连接
            while not self._pool.empty():
                try:
                    conn_info = self._pool.get_nowait()
                    conn_info['client'].close()
                except:
                    pass

            self._connection_count = 0

    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self._lock:
            return {
                'total_connections': self._connection_count,
                'active_connections': len(self._active_connections),
                'available_connections': self._pool.qsize(),
                'max_connections': self.max_connections,
                'min_connections': self.min_connections,
                'waiting_requests': self._waiting_queue.qsize()
            }

    def resize_pool(self, new_max_connections: int):
        """动态调整连接池大小"""
        with self._lock:
            if new_max_connections < self.min_connections:
                raise ValueError("Max connections cannot be less than min connections")

            old_max = self.max_connections
            self.max_connections = new_max_connections

            print(f"Connection pool resized from {old_max} to {new_max_connections}")


class ConnectionManager:
    _pool: Optional[FlightConnectionPool] = None

    @staticmethod
    def set_connection_pool(pool: FlightConnectionPool):
        ConnectionManager._pool = pool

    @staticmethod
    def get_connection(timeout: Optional[float] = None) -> FlightClient:
        if ConnectionManager._pool is None:
            raise RuntimeError("Connection pool has not been initialized.")
        return ConnectionManager._pool.get_client(timeout=timeout)

    @staticmethod
    def close_connection_pool():
        print(f"Closing connection pool: {ConnectionManager._pool}")
        if ConnectionManager._pool:
            ConnectionManager._pool.close_all()
            ConnectionManager._pool = None

    @staticmethod
    def get_pool_stats():
        if ConnectionManager._pool:
            return ConnectionManager._pool.get_stats()
        return None


atexit.register(ConnectionManager.close_connection_pool)


def stress_test_connection_pool():
    """强制测试连接池满的情况"""
    # 创建小容量连接池的客户端
    ConnectionManager.set_connection_pool(FlightConnectionPool("grpc://localhost:8080", max_connections=20, idle_timeout=30))

    print("=== 开始连接池压力测试 ===")
    print("初始连接池状态:", ConnectionManager._pool.get_stats())

    # 创建事件用于同步线程
    start_event = threading.Event()
    end_events = [threading.Event() for _ in range(100)]

    def worker(worker_id, end_event):
        """工作线程函数，故意长时间持有连接"""
        print(f"Worker {worker_id} 等待开始...")
        start_event.wait()  # 等待所有线程就绪

        try:
            print(f"Worker {worker_id} 尝试获取连接...")
            with ConnectionManager.get_connection() as conn:
               print(f"Worker {worker_id} 获取到连接 | 当前池状态:", ConnectionManager._pool.get_stats())

               # 模拟长时间操作
               time.sleep(3)
               flights = list(conn.list_flights())
               print(f"Worker {worker_id} 完成操作，找到 {len(flights)} flights")

        except Exception as e:
            print(f"Worker {worker_id} 出错: {e}")
        finally:
            end_event.set()  # 标记完成

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for i in range(100):
            future = executor.submit(worker, i, end_events[i])
            futures.append(future)

        # 给线程一点时间启动
        time.sleep(1)
        print("\n所有线程准备就绪，即将同时请求连接...\n")
        start_event.set()  # 同时释放所有线程

        # 等待所有线程完成
        for event in end_events:
            event.wait()

    print("\n=== 测试完成 ===")
    print("最终连接池状态:", ConnectionManager._pool.get_stats())

    ConnectionManager._pool.close_all()  # 清理连接池


# if __name__ == "__main__":
#     stress_test_connection_pool()