import pyarrow.flight as flight

def test_flight_connection():
    # 连接到本地运行的服务
    client = flight.connect("grpc://localhost:3101")

    try:
        # 获取服务上的所有可用 endpoint（可选）
        endpoints = list(client.list_flights())
        print("Available flights:")
        for endpoint in endpoints:
            print(endpoint)

        # 示例调用某个 action 或直接 ping
        result = client.do_action(flight.Action("health_check", b""))
        for r in result:
            print("Health check response:", r.body.to_pybytes().decode())

    except Exception as e:
        print("Error during testing:", str(e))

if __name__ == "__main__":
    test_flight_connection()
