import pyarrow as pa
import pyarrow.ipc as ipc
import os
from parser.abstract_parser import BaseParser
import logging
import numpy as np
import tifffile

logger = logging.getLogger(__name__)

class TIFParser(BaseParser):
    """
    通用 TIFF/GeoTIFF 解析器，支持多页、多维、不同波段排列等情况。
    可读取 TIFF 并转为 Arrow Table，也可从 Arrow Table 写回 TIFF。
    """

    def parse(self, file_path: str) -> pa.Table:
        """
        将任意 TIFF 文件解析为 Arrow Table，并附带元数据，并缓存为 .arrow 文件后再读取。
        对于shape不一致的多页/多band，自动用NaN补齐。
        """
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/tif/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        try:
            if os.path.exists(arrow_file_path):
                logger.info(f"检测到缓存文件，直接从 {arrow_file_path} 读取 Arrow Table。")
                with pa.memory_map(arrow_file_path, "r") as source:
                    return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取缓存 .arrow 文件失败: {e}")

        try:
            logger.info(f"开始读取 TIFF 文件: {file_path}")
            with tifffile.TiffFile(file_path) as tif:
                images = [page.asarray() for page in tif.pages]
                logger.info(f"TIFF文件包含 {len(images)} 页")
                shapes = [img.shape for img in images]
                dtypes = [str(img.dtype) for img in images]
                pa_arrays_raw = []
                band_names = []
                orig_lengths = []
                # 先收集所有band的原始数据
                for idx, img in enumerate(images):
                    if img.ndim == 2:
                        arr = img.flatten().astype(np.float64)
                        pa_arrays_raw.append(arr)
                        band_names.append(f'page{idx+1}_band1')
                        orig_lengths.append(arr.size)
                    elif img.ndim == 3:
                        # (B, H, W)
                        if img.shape[0] in [1, 3, 4] and img.shape[0] < img.shape[1] and img.shape[0] < img.shape[2]:
                            for b in range(img.shape[0]):
                                arr = img[b, :, :].flatten().astype(np.float64)
                                pa_arrays_raw.append(arr)
                                band_names.append(f'page{idx+1}_band{b+1}')
                                orig_lengths.append(arr.size)
                        # (H, W, B)
                        elif img.shape[2] in [1, 3, 4] and img.shape[2] < img.shape[0] and img.shape[2] < img.shape[1]:
                            for b in range(img.shape[2]):
                                arr = img[:, :, b].flatten().astype(np.float64)
                                pa_arrays_raw.append(arr)
                                band_names.append(f'page{idx+1}_band{b+1}')
                                orig_lengths.append(arr.size)
                        else:
                            arr = img.flatten().astype(np.float64)
                            pa_arrays_raw.append(arr)
                            band_names.append(f'page{idx+1}_flatten')
                            orig_lengths.append(arr.size)
                    else:
                        arr = img.flatten().astype(np.float64)
                        pa_arrays_raw.append(arr)
                        band_names.append(f'page{idx+1}_flatten')
                        orig_lengths.append(arr.size)
                # 用NaN补齐
                max_len = max(len(arr) for arr in pa_arrays_raw)
                pa_arrays = []
                for arr in pa_arrays_raw:
                    if len(arr) < max_len:
                        padded = np.full(max_len, np.nan, dtype=np.float64)
                        padded[:len(arr)] = arr
                        pa_arrays.append(pa.array(padded))
                    else:
                        pa_arrays.append(pa.array(arr))
                # 合成Arrow Table
                table = pa.table(pa_arrays, names=band_names)
                # 合并所有页的元数据
                meta = {}
                for i, page in enumerate(tif.pages):
                    for tag in page.tags.values():
                        meta[f'page{i+1}_{tag.name}'] = str(tag.value)
                meta['shapes'] = str(shapes)
                meta['dtypes'] = str(dtypes)
                meta['orig_lengths'] = str(orig_lengths)
                table = table.replace_schema_metadata(meta)
        except Exception as e:
            logger.error(f"解析 TIFF 文件失败: {e}")
            raise

        try:
            logger.info(f"保存 Arrow Table 到 {arrow_file_path}")
            with ipc.new_file(arrow_file_path, table.schema) as writer:
                writer.write_table(table)
        except Exception as e:
            logger.error(f"保存 .arrow 文件失败: {e}")
            raise

        try:
            logger.info(f"从 .arrow 文件 {arrow_file_path} 读取 Arrow Table。")
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取 .arrow 文件失败: {e}")
            raise

    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写入 TIFF 文件。
        支持多页、多波段、多shape的还原（需依赖metadata）。
        写回时自动去除NaN补齐部分，只用有效数据还原 shape。
        """
        try:
            meta = table.schema.metadata or {}
            # 还原shape、dtype、原始长度
            shapes = eval(meta.get(b'shapes', b'[]').decode() if isinstance(meta.get(b'shapes', b''), bytes) else meta.get('shapes', '[]'))
            dtypes = eval(meta.get(b'dtypes', b'[]').decode() if isinstance(meta.get(b'dtypes', b''), bytes) else meta.get('dtypes', '[]'))
            orig_lengths = eval(meta.get(b'orig_lengths', b'[]').decode() if isinstance(meta.get(b'orig_lengths', b''), bytes) else meta.get('orig_lengths', '[]'))
            arrays = [col.to_numpy() for col in table.columns]
            images = []
            arr_idx = 0
            for i, shape in enumerate(shapes):
                dtype = np.dtype(dtypes[i])
                if len(shape) == 2:
                    # 单波段
                    valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                    img = valid.reshape(shape).astype(dtype)
                    images.append(img)
                    arr_idx += 1
                elif len(shape) == 3:
                    bands = shape[0] if (shape[0] in [1, 3, 4] and shape[0] < shape[1] and shape[0] < shape[2]) else shape[2]
                    band_imgs = []
                    for b in range(bands):
                        valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                        if bands == shape[0]:
                            band_imgs.append(valid.reshape((shape[1], shape[2])).astype(dtype))
                        else:
                            band_imgs.append(valid.reshape((shape[0], shape[1])).astype(dtype))
                        arr_idx += 1
                    if bands == shape[0]:
                        img = np.stack(band_imgs, axis=0)
                    else:
                        img = np.stack(band_imgs, axis=-1)
                    images.append(img)
                else:
                    valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                    img = valid.reshape(shape).astype(dtype)
                    images.append(img)
                    arr_idx += 1
            logger.info(f"写入 TIFF 文件到 {output_path}，共 {len(images)} 页")
            tifffile.imwrite(output_path, images)
        except Exception as e:
            logger.error(f"写入 TIFF 文件失败: {e}")
            raise
    
    def sample(self, file_path):
        """
        从 TIFF 文件中采样数据，返回 Arrow Table。
        这里简单实现为读取第一页（或第一波段）前max_rows个像素，自动补齐为相同长度，并添加schema的metadata。
        max_rows: 生成的Arrow Table的行数，默认20
        """
        try:
            max_rows=20
            logger.info(f"开始采样 TIFF 文件: {file_path}")
            with tifffile.TiffFile(file_path) as tif:
                if len(tif.pages) == 0:
                    logger.error("TIFF 文件无有效页")
                    raise ValueError("TIFF 文件无有效页")
                img = tif.pages[0].asarray()
                logger.info(f"第一页 shape: {img.shape}, dtype: {img.dtype}")
                arrays = []
                names = []
                shapes = []
                dtypes = []
                orig_lengths = []
                if img.ndim == 2:
                    arr = img.flatten().astype(np.float64)
                    arrays.append(arr[:max_rows])
                    names.append('page1_band1')
                    shapes.append(img.shape)
                    dtypes.append(str(img.dtype))
                    orig_lengths.append(min(max_rows, arr.size))
                    logger.info("采样二维影像，波段数: 1")
                elif img.ndim == 3:
                    # (B, H, W)
                    if img.shape[0] in [1, 3, 4] and img.shape[0] < img.shape[1] and img.shape[0] < img.shape[2]:
                        logger.info(f"采样三维影像，按(B, H, W)模式，波段数: {img.shape[0]}")
                        for b in range(img.shape[0]):
                            arr = img[b, :, :].flatten().astype(np.float64)
                            arrays.append(arr[:max_rows])
                            names.append(f'page1_band{b+1}')
                            shapes.append(img[b, :, :].shape)
                            dtypes.append(str(img.dtype))
                            orig_lengths.append(min(max_rows, arr.size))
                    # (H, W, B)
                    elif img.shape[2] in [1, 3, 4] and img.shape[2] < img.shape[0] and img.shape[2] < img.shape[1]:
                        logger.info(f"采样三维影像，按(H, W, B)模式，波段数: {img.shape[2]}")
                        for b in range(img.shape[2]):
                            arr = img[:, :, b].flatten().astype(np.float64)
                            arrays.append(arr[:max_rows])
                            names.append(f'page1_band{b+1}')
                            shapes.append(img[:, :, b].shape)
                            dtypes.append(str(img.dtype))
                            orig_lengths.append(min(max_rows, arr.size))
                    else:
                        logger.info("采样三维影像，未知排列，直接flatten")
                        arr = img.flatten().astype(np.float64)
                        arrays.append(arr[:max_rows])
                        names.append('page1_flatten')
                        shapes.append(img.shape)
                        dtypes.append(str(img.dtype))
                        orig_lengths.append(min(max_rows, arr.size))
                else:
                    logger.info("采样高维影像，直接flatten")
                    arr = img.flatten().astype(np.float64)
                    arrays.append(arr[:max_rows])
                    names.append('page1_flatten')
                    shapes.append(img.shape)
                    dtypes.append(str(img.dtype))
                    orig_lengths.append(min(max_rows, arr.size))
                # 补齐
                max_len = max_rows
                pa_arrays = []
                for arr in arrays:
                    if len(arr) < max_len:
                        padded = np.full(max_len, np.nan, dtype=np.float64)
                        padded[:len(arr)] = arr
                        pa_arrays.append(pa.array(padded))
                    else:
                        pa_arrays.append(pa.array(arr))
                # 构造schema并添加metadata
                schema = pa.schema([pa.field(n, pa.float64()) for n in names])
                meta = {
                    "shapes": str(shapes),
                    "dtypes": str(dtypes),
                    "orig_lengths": str(orig_lengths),
                    "file_type": "TIFF",
                    "sample": "True"
                }
                schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})
                table = pa.table(pa_arrays, schema=schema)
                logger.info(f"采样完成，生成 Arrow Table，列: {names}，每列采样长度: {max_len}，orig_lengths: {orig_lengths}")
                return table
        except Exception as e:
            logger.error(f"采样 TIFF 文件失败: {e}")
            raise