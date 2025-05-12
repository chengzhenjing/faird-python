import os, sys
from core.config import FairdConfigManager

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from services.server.faird_server import FairdServer

def main():
    ## Load configuration
    FairdConfigManager.load_config(os.path.join(current_dir, 'faird.conf'))
    config = FairdConfigManager.get_config()
    if not config:
        print("Failed to load configuration.")
        return

    FairdServer.create(host=config.domain, port=config.port)

if __name__ == "__main__":
    main()