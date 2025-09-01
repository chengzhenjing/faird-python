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

    def count(self, file_path: str) -> int:
        raise NotImplementedError("CSVParser.count method is not implemented yet.")