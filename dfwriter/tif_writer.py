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
        支持多页、多波段、多shape的还原（需依赖metadata）。
        写回时自动去除NaN补齐部分，只用有效数据还原 shape。
        """
        try:
            logger.info(f"开始写入 TIFF 文件: {output_path}")
            meta = table.schema.metadata or {}
            # 还原shape、dtype、原始长度
            try:
                shapes = eval(meta.get(b'shapes', b'[]').decode() if isinstance(meta.get(b'shapes', b''), bytes) else meta.get('shapes', '[]'))
                dtypes = eval(meta.get(b'dtypes', b'[]').decode() if isinstance(meta.get(b'dtypes', b''), bytes) else meta.get('dtypes', '[]'))
                orig_lengths = eval(meta.get(b'orig_lengths', b'[]').decode() if isinstance(meta.get(b'orig_lengths', b''), bytes) else meta.get('orig_lengths', '[]'))
                band_names = eval(meta.get(b'band_names', b'[]').decode() if isinstance(meta.get(b'band_names', b''), bytes) else meta.get('band_names', '[]'))
            except Exception as e:
                logger.error(f"元数据解析异常: {e}")
                raise
            try:
                arrays = [col.to_numpy() for col in table.columns]
            except Exception as e:
                logger.error(f"Arrow Table 转 numpy 异常: {e}")
                raise
            images = []
            arr_idx = 0
            i = 0
            try:
                while i < len(shapes):
                    shape = shapes[i]
                    dtype = np.dtype(dtypes[i])
                    if len(shape) == 2:
                        valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                        img = valid.reshape(shape).astype(dtype)
                        images.append(img)
                        arr_idx += 1
                        i += 1
                    elif len(shape) == 3:
                        # 判断是 (B, H, W) 还是 (H, W, B)
                        if shape[0] in [1, 3, 4] and shape[0] < shape[1] and shape[0] < shape[2]:
                            bands = shape[0]
                            band_imgs = []
                            for b in range(bands):
                                valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                                band_imgs.append(valid.reshape((shape[1], shape[2])).astype(dtype))
                                arr_idx += 1
                                i += 1
                            img = np.stack(band_imgs, axis=0)
                            images.append(img)
                        elif shape[2] in [1, 3, 4] and shape[2] < shape[0] and shape[2] < shape[1]:
                            bands = shape[2]
                            band_imgs = []
                            for b in range(bands):
                                valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                                band_imgs.append(valid.reshape((shape[0], shape[1])).astype(dtype))
                                arr_idx += 1
                                i += 1
                            img = np.stack(band_imgs, axis=-1)
                            images.append(img)
                        else:
                            valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                            img = valid.reshape(shape).astype(dtype)
                            images.append(img)
                            arr_idx += 1
                            i += 1
                    else:
                        valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                        img = valid.reshape(shape).astype(dtype)
                        images.append(img)
                        arr_idx += 1
                        i += 1
            except Exception as e:
                logger.error(f"TIFF 还原 numpy 数据异常: {e}")
                raise
            try:
                tifffile.imwrite(output_path, images if len(images) > 1 else images[0])
                logger.info(f"写入 TIFF 文件到 {output_path}，共 {len(images)} 页")
            except Exception as e:
                logger.error(f"写入 TIFF 文件异常: {e}")
                raise
        except Exception as e:
            logger.error(f"写入 TIFF 文件失败: {e}")
            raise