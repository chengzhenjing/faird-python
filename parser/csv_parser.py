import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import os

from parser.abstract_parser import BaseParser


class CSVParser(BaseParser):
    """
    CSV file parser implementing the BaseParser interface.
    """

    def parse(self, file_path: str) -> pa.Table:
        """
        Save the CSV file as a .arrow file and load it into memory as a pyarrow Table using zero-copy.

        Args:
            file_path (str): Path to the input CSV file.
        Returns:
            pa.Table: A pyarrow Table object.
        """

        # Ensure the cache directory exists
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/csv/")
        os.makedirs(os.path.dirname(DEFAULT_ARROW_CACHE_PATH), exist_ok=True)

        # Extract the file name and append the .arrow suffix
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # Read the CSV file into a pyarrow Table
        table = csv.read_csv(file_path)

        # Save the table as a .arrow file
        with ipc.new_file(arrow_file_path, table.schema) as writer:
            writer.write_table(table)
        print(f"成功将 {file_path} 保存为 {arrow_file_path}")

        # Load the .arrow file into a pyarrow Table using zero-copy
        with pa.memory_map(arrow_file_path, "r") as source:
            return ipc.open_file(source).read_all()