import pyarrow as pa
import pyarrow.flight as flight
from services.server.faird_service_producer import FairdServiceProducer

class FairdServer:
    service_producer = None

    def __init__(self, service_producer):
        self.service_producer = service_producer

    @staticmethod
    def create(host: str = "localhost", port: int = 8815):
        location = flight.Location.for_grpc_tcp(host, port)
        try:
            server = FairdServiceProducer(location)
            print(f"S: Listening on {host}:{port}")
            server.serve()
            server.wait_for_termination()
        except Exception as e:
            print(e)