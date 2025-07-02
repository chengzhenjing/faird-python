from io import BytesIO

from parser.abstract_parser import BaseParser
import pyarrow as pa
import pyarrow.ipc as ipc
from utils.logger_utils import get_logger
logger = get_logger(__name__)

class DefaultParser(BaseParser):

    def parse(self, file_path: str) -> None:
        raise NotImplementedError("CSVParser.parse method is not implemented yet.")

    def sample(self, file_path: str) -> None:
        raise NotImplementedError("CSVParser.sample method is not implemented yet.")

    def write(self, table: None, output_path: str) -> None:
        try:
            if isinstance(output_path, BytesIO):
                # 使用 BufferOutputStream 写入字节流
                sink = pa.BufferOutputStream()
                with ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)
                # 将数据写入 BytesIO
                output_path.write(sink.getvalue().to_pybytes())
            else:
                logger.info(f"即将写入 arrow 文件: {output_path}")
                with pa.OSFile(output_path, 'wb') as sink:
                    with ipc.new_file(sink, table.schema) as writer:
                        writer.write_table(table)
                logger.info(f"成功写入 arrow 文件: {output_path}")
        except Exception as e:
            logger.error(f"写入 arrow 文件时出错: {e}")
            raise

    def count(self, file_path: str) -> int:
        raise NotImplementedError("CSVParser.count method is not implemented yet.")