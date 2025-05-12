from abc import ABC, abstractmethod
import pyarrow as pa

class BaseParser(ABC):
    """
    Abstract base class defining the interface for all parsers.
    """

    @abstractmethod
    def parse(self, file_path: str) -> pa.Table:
        """
        Parse the input file into a pyarrow.Table.

        Args:
            file_path (str): Path to the input file.
        Returns:
            pa.Table: A pyarrow Table object.
        """
        pass