from abc import ABC, abstractmethod
import pyarrow as pa


class BaseWriter(ABC):
    """
    Abstract base class defining the interface for all parsers.
    """

    @abstractmethod
    def write(self, table: pa.Table, output_path: str):
        """
        Write the pyarrow.Table back to the original file format.

        Args:
            table (pa.Table): The Arrow Table to write.
            output_path (str): Output file path.
        """
        pass