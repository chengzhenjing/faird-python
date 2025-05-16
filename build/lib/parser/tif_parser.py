import os
import rasterio
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from parser.abstract_parser import BaseParser


class TIFParser(BaseParser):
    """
    TIFF/GeoTIFF file parser implementing the BaseParser interface.
    """

    def parse(self, file_path: str) -> pa.Table:
        """
        Parse a TIFF/GeoTIFF file into a pyarrow Table.

        Args:
            file_path (str): Path to the input TIFF/GeoTIFF file.
        Returns:
            pa.Table: A pyarrow Table object representing pixel values for each band.
        """

        # 设置缓存路径
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/tif/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)

        # 构造缓存文件路径
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # 如果缓存存在，直接从缓存加载
        if os.path.exists(arrow_file_path):
            print(f"从缓存加载 {arrow_file_path}")
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()

        # 打开 TIFF 文件
        with rasterio.open(file_path) as src:
            num_bands = src.count
            height, width = src.height, src.width
            data = []

            # 逐波段读取数据
            for i in range(1, num_bands + 1):
                band_data = src.read(i)
                data.append(band_data.flatten())  # 展平为一维数组

            # 转换为 PyArrow 数组
            arrays = [pa.array(d, type=pa.from_numpy_dtype(d.dtype)) for d in data]
            names = [f"band_{i}" for i in range(1, num_bands + 1)]

            # 创建 Table
            table = pa.Table.from_arrays(arrays, names=names)

            # 写入缓存
            with ipc.new_file(arrow_file_path, table.schema) as writer:
                writer.write_table(table)
            print(f"成功将 {file_path} 保存为 {arrow_file_path}")

            # 零拷贝读取返回
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()

    def write(self, table: pa.Table, output_path: str):
        """
        占位 write 方法，用于满足 BaseParser 接口要求。
        当前尚未实现写入功能。

        Args:
            table (pa.Table): 要写入的数据（当前不处理）
            output_path (str): 目标输出路径（当前不处理）
        Raises:
            NotImplementedError: 始终抛出未实现异常
        """
        raise NotImplementedError("TIFParser.write() 尚未实现：当前不支持写回 TIF 文件")
