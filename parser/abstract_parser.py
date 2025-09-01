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
    
    @abstractmethod
    def sample(self, file_path:str) -> pa.Table:
        """
        Sample the input file and return a pyarrow.Table.
        This method can be overridden by subclasses to provide sampling functionality.
        
        Args:
            file_path (str): Path to the input file.
        
        Returns:
            pa.Table: A pyarrow Table object containing sampled data.
        """
        pass
    
    @abstractmethod
    def count(self, file_path: str) -> int:
        """
        返回解析后 Arrow Table 的总行数。

        Args:
            file_path (str): 输入文件路径。

        Returns:
            int: Arrow Table 的总行数。
        """
        pass