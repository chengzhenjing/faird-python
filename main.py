import os, sys
from core.config import FairdConfigManager
import time
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

    print("This will be buffered by default.")
    print("This will be flushed immediately.", flush=True)  # Python 3.x

    time.sleep(1)

    print("Another message.")
    sys.stdout.flush()  # 强制刷新标准输出缓冲区
    sys.stderr.flush()  # 如果有错误输出也可能需要刷新

    FairdServer.create(host=config.domain, port=config.port)

if __name__ == "__main__":
    main()