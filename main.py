
import os
import sys
import logging
import time

# 配置 logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler("faird.log"),  # 输出到日志文件
        logging.StreamHandler()            # 同时输出到控制台
    ]
)

# 获取 logger
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from core.config import FairdConfigManager
from services.server.faird_server import FairdServer

def main():
    ## Load configuration
    FairdConfigManager.load_config(os.path.join(current_dir, 'faird.conf'))
    # FairdConfigManager.load_config(os.path.join(current_dir, 'faird-dev.conf'))
    config = FairdConfigManager.get_config()
    if not config:
        logger.error("Failed to load configuration.")
        return
    logger.info("Application is starting up...")
    FairdServer.create(host=config.domain, port=config.port)

if __name__ == "__main__":
    main()