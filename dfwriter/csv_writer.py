import pyarrow as pa
import pyarrow.ipc as ipc
import os

from dfwriter.abstract_writer import BaseWriter
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class CSVWriter(BaseWriter):
    """
    CSV file writer implementing the BaseWriter interface.
    """

    def write(self, table: pa.Table, output_path: str):
        try:
            logger.info(f"即将写入 CSV 文件: {output_path}")
            # 将 Arrow Table 转换为 Pandas DataFrame
            df = table.to_pandas()
            # 写入 CSV 文件
            df.to_csv(output_path, index=False)
            logger.info(f"成功写入 CSV 文件: {output_path}")
        except Exception as e:
            logger.error(f"写入 CSV 文件时出错: {e}")
            raise