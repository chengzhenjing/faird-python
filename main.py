import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from services.faird_service import get_flight_server

def main():
    flight_server = get_flight_server()
    flight_server.serve()

if __name__ == "__main__":
    main()