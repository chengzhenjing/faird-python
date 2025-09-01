import pyarrow as pa
import pyarrow.ipc as ipc
import os
import numpy as np
import tifffile

from dfwriter.abstract_writer import BaseWriter
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class TIFWriter(BaseWriter):
    """
    tif file writer implementing the BaseWriter interface.
    """

    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写入 TIFF 文件。
        通过读取元数据中的 'page_shapes' 来精确还原。
        """
        try:
            logger.info(f"开始写入 TIFF 文件: {output_path}")
            meta = table.schema.metadata or {}
            try:
                page_shapes = eval(meta.get(b'page_shapes', b'[]').decode())
                dtypes = eval(meta.get(b'dtypes', b'[]').decode())
                orig_lengths = eval(meta.get(b'orig_lengths', b'[]').decode())
            except Exception as e:
                logger.error(f"元数据解析异常: {e}")
                raise

            arrays = [col.to_numpy() for col in table.columns]
            images = []
            arr_idx = 0

            # --- 修复核心：基于 page_shapes 循环进行还原 ---
            for i, page_shape in enumerate(page_shapes):
                dtype = np.dtype(dtypes[i])

                # 情况一：原始页面是 2D (单波段)
                if len(page_shape) == 2:
                    if arr_idx >= len(arrays): raise IndexError("列数据不足以还原指定的页面形状")
                    valid_data = arrays[arr_idx][:orig_lengths[arr_idx]]
                    img = valid_data.reshape(page_shape).astype(dtype)
                    images.append(img)
                    arr_idx += 1

                # 情况二：原始页面是 3D (多波段)
                elif len(page_shape) == 3:
                    # 从已知的正确形状中确定波段数和排列方式
                    # Planar: (B, H, W) - 波段数在第0轴
                    if page_shape[0] < page_shape[1] and page_shape[0] < page_shape[2]:
                        num_bands = page_shape[0]
                        stack_axis = 0
                        h, w = page_shape[1], page_shape[2]
                    # Interleaved/Chunky: (H, W, B) - 波段数在最后1轴
                    else:
                        num_bands = page_shape[2]
                        stack_axis = -1
                        h, w = page_shape[0], page_shape[1]

                    if arr_idx + num_bands > len(arrays): raise IndexError("列数据不足以还原指定的多波段页面")

                    band_imgs = []
                    for _ in range(num_bands):
                        valid_data = arrays[arr_idx][:orig_lengths[arr_idx]]
                        band_img = valid_data.reshape((h, w)).astype(dtype)
                        band_imgs.append(band_img)
                        arr_idx += 1

                    # 按照正确的轴堆叠回多波段图像
                    img = np.stack(band_imgs, axis=stack_axis)
                    images.append(img)

                # 其他更高维度的情况
                else:
                    if arr_idx >= len(arrays): raise IndexError("列数据不足以还原指定的页面形状")
                    valid_data = arrays[arr_idx][:orig_lengths[arr_idx]]
                    img = valid_data.reshape(page_shape).astype(dtype)
                    images.append(img)
                    arr_idx += 1

            tifffile.imwrite(output_path, images if len(images) > 1 else images[0])
            logger.info(f"写入 TIFF 文件到 {output_path}，共 {len(images)} 页")
        except Exception as e:
            logger.error(f"写入 TIFF 文件失败: {e}")
            raise