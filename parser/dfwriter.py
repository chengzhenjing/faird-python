import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from parser.csv_parser import CSVParser
from parser.nc_parser import NCParser
from parser.tif_parser import TIFParser
from parser.default_parser import DefaultParser


class DfWriter:
    def __init__(self):
        self.output_target = None
        self.output_format = "arrow"  # 默认格式
        self.parser_switch = {
            "csv": CSVParser,
            "nc": NCParser,
            "tif": TIFParser,
            "tiff": TIFParser,
            "arrow": DefaultParser,
        }

    def output(self, target):
        """
        设置输出目标，可以是文件路径、文件流或字节流
        """
        self.output_target = target
        return self

    def format(self, fmt):
        """
        设置输出格式
        """
        self.output_format = fmt.lower()
        return self

    def write(self, df):
        parser_class = self.parser_switch.get(self.output_format)
        if not parser_class:
            raise ValueError(f"Unsupported format: {self.output_format}")
        parser = parser_class()
        df.collect()
        arrow_table = df.data
        parser.write(table=arrow_table, output_path=self.output_target)


# 示例用法
if __name__ == "__main__":
    # 创建示例 DataFrame
    data = {"name": ["Alice", "Bob"], "age": [25, 30]}
    df = pd.DataFrame(data)

    # 输出到 CSV 文件
    writer = DfWriter()
    writer.output("output.csv").format("csv").write(df)

    # 输出到 JSON 文件
    writer.output("output.json").format("json").write(df)

    # 输出到 HDF5 文件
    writer.output("output.arrow").write(df) # 默认是ARROW格式

    # 输出到字节流（ARROW 格式）
    buffer = BytesIO()
    writer.output(buffer).format("arrow").write(df)
    print("Arrow data written to buffer:", buffer.getvalue())