import os
import rasterio
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from parser.abstract_parser import BaseParser
import logging
logger = logging.getLogger(__name__)

class TIFParser(BaseParser):
    """
    通用 TIFF/GeoTIFF 解析器，支持任意波段数、任意图像大小。
    可读取 TIFF 并转为 Arrow Table，也可从 Arrow Table 写回 TIFF。
    """

    def parse(self, file_path: str) -> dict:
        """
        将任意 TIFF 文件解析为 Arrow Table，并附带元数据。

        Args:
            file_path (str): 输入 TIFF 文件路径
        Returns:
            dict: 包含 Arrow Table 和元数据的对象
        """

        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/tif/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)

        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # 缓存加载
        if os.path.exists(arrow_file_path):
            logger.info(f"🔁 从缓存加载 {arrow_file_path}")
            try:
                with pa.memory_map(arrow_file_path, "r") as source:
                    table = ipc.open_file(source).read_all()
                return {
                    "table": table,
                    "metadata": self._load_metadata(file_path)
                }
            except Exception as e:
                logger.info(f"🚨 缓存加载失败: {e}")

        # 解析文件
        logger.info(f"📂 正在解析 TIFF 文件: {file_path}")
        with rasterio.open(file_path) as src:
            num_bands = src.count
            height, width = src.height, src.width
            dtype = src.dtypes[0]

            data = []
            names = []

            for i in range(1, num_bands + 1):
                band_data = src.read(i)
                data.append(band_data.flatten())
                names.append(f"band_{i}")

            arrays = [pa.array(d, type=pa.from_numpy_dtype(d.dtype)) for d in data]
            table = pa.Table.from_arrays(arrays, names=names)

            # 写入缓存
            logger.info(f"💾 写入缓存文件: {arrow_file_path}")
            try:
                with ipc.new_file(arrow_file_path, table.schema) as writer:
                    writer.write_table(table)
            except Exception as e:
                logger.info(f"❌ 缓存写入失败: {e}")

        metadata = self._load_metadata(file_path, src)

        return {
            "table": table,
            "metadata": metadata
        }

    def _load_metadata(self, file_path: str, src=None):
        """提取 TIFF 文件的元数据"""
        if src is None:
            with rasterio.open(file_path) as src:
                return {
                    "width": src.width,
                    "height": src.height,
                    "count": src.count,
                    "dtype": src.dtypes[0],
                    "crs": src.crs.to_string() if src.crs else None,
                    "transform": list(src.transform),
                    "driver": src.driver,
                    "nodata": src.nodata,
                }
        else:
            return {
                "width": src.width,
                "height": src.height,
                "count": src.count,
                "dtype": src.dtypes[0],
                "crs": src.crs.to_string() if src.crs else None,
                "transform": list(src.transform),
                "driver": src.driver,
                "nodata": src.nodata,
            }

    def write(self, parsed_data: dict, output_path: str):
        """
        将 parse 返回的 dict 对象写回 GeoTIFF 文件。

        Args:
            parsed_data (dict): 包含 Arrow Table 和元数据的对象
            output_path (str): 输出文件路径
        """
        table = parsed_data["table"]
        meta = parsed_data["metadata"]

        num_bands = len(table.column_names)
        width = meta["width"]
        height = meta["height"]
        dtype = meta.get("dtype", "float32")
        crs = meta.get("crs")
        transform = meta.get("transform")

        # 构建二维数组
        bands = []
        for col in table.column_names:
            flat_array = table[col].to_numpy()
            reshaped = flat_array.reshape((height, width))
            bands.append(reshaped)

        # 构建 RasterIO 元数据
        profile = {
            'driver': meta.get('driver', 'GTiff'),
            'height': height,
            'width': width,
            'count': num_bands,
            'dtype': dtype,
            'nodata': meta.get('nodata'),
            'transform': rasterio.Affine(*transform) if transform else rasterio.Affine.identity(),
            'crs': crs or 'EPSG:4326',
            'compress': 'lzw'
        }

        logger.info(f"💾 正在写入 GeoTIFF 文件: {output_path}")
        with rasterio.open(output_path, 'w', **profile) as dst:
            for i, band_data in enumerate(bands, start=1):
                dst.write(band_data, i)

        logger.info(f"✅ 成功写回 GeoTIFF 文件: {output_path}")

