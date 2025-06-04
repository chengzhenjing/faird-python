import pyarrow as pa
import pyarrow.ipc as ipc
import os
from parser.abstract_parser import BaseParser
import logging
import numpy as np
import netCDF4

logger = logging.getLogger(__name__)

class NCParser(BaseParser):
    """
    通用 NC 解析器，支持多变量、多维度等情况。
    可读取 NC 并转为 Arrow Table，也可从 Arrow Table 写回 NC。
    """

    def parse(self, file_path: str) -> pa.Table:
        """
        将 NetCDF 文件解析为 Arrow Table，并缓存为 .arrow 文件后再读取。
        支持多变量、多维度，自动补齐不同长度为NaN。
        记录每个变量的shape、dtype、属性和全局属性。
        """
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/nc/")
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
            logger.info(f"开始读取 NetCDF 文件: {file_path}")
            ds = netCDF4.Dataset(file_path, 'r')
            var_names = [v for v in ds.variables if ds.variables[v].ndim > 0]
            arrays_raw = []
            col_names = []
            orig_shapes = []
            dtypes = []
            var_attrs = {}
            fill_values = {}
            orig_lengths = []
            for v in var_names:
                arr = ds.variables[v][:]
                arr_flat = np.array(arr).flatten()
                arrays_raw.append(arr_flat)
                col_names.append(v)
                orig_shapes.append(arr.shape)
                dtypes.append(str(arr.dtype))
                orig_lengths.append(len(arr_flat))
                # 记录变量属性
                attrs = {k: ds.variables[v].getncattr(k) for k in ds.variables[v].ncattrs()}
                var_attrs[v] = attrs
                # 记录缺测值
                fill_value = attrs.get('_FillValue', None)
                fill_values[v] = fill_value
            # 用NaN补齐
            max_len = max(len(arr) for arr in arrays_raw)
            pa_arrays = []
            for arr in arrays_raw:
                if len(arr) < max_len:
                    padded = np.full(max_len, np.nan, dtype=np.float64)
                    padded[:len(arr)] = arr.astype(np.float64)
                    pa_arrays.append(pa.array(padded))
                else:
                    pa_arrays.append(pa.array(arr.astype(np.float64)))
            table = pa.table(pa_arrays, names=col_names)
            # 保存元数据
            meta = {
                "shapes": str(orig_shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str({k: ds.getncattr(k) for k in ds.ncattrs()}),
                "orig_lengths": str(orig_lengths)
            }
            table = table.replace_schema_metadata(meta)
            ds.close()
        except Exception as e:
            logger.error(f"解析 NetCDF 文件失败: {e}")
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
        将 Arrow Table 写回 NetCDF 文件。
        支持变量属性、全局属性、缺测值、原始dtype和shape的还原。
        """
        try:
            meta = table.schema.metadata or {}

            def _meta_eval(val, default):
                if isinstance(val, bytes):
                    return eval(val.decode())
                elif isinstance(val, str):
                    return eval(val)
                else:
                    return default

            def get_meta(meta, key, default):
                if key in meta:
                    return meta[key]
                if isinstance(key, str) and key.encode() in meta:
                    return meta[key.encode()]
                if isinstance(key, bytes) and key.decode() in meta:
                    return meta[key.decode()]
                return default

            shapes = _meta_eval(get_meta(meta, 'shapes', '[]'), [])
            dtypes = _meta_eval(get_meta(meta, 'dtypes', '[]'), [])
            var_names = _meta_eval(get_meta(meta, 'var_names', '[]'), [])
            var_attrs = _meta_eval(get_meta(meta, 'var_attrs', '{}'), {})
            fill_values = _meta_eval(get_meta(meta, 'fill_values', '{}'), {})
            global_attrs = _meta_eval(get_meta(meta, 'global_attrs', '{}'), {})
            orig_lengths = _meta_eval(get_meta(meta, 'orig_lengths', '[]'), [])
            arrays = [col.to_numpy() for col in table.columns]

            # 检查长度一致性
            if not (len(var_names) == len(shapes) == len(dtypes) == len(orig_lengths) == len(arrays)):
                raise ValueError(
                    f"元数据长度不一致: var_names({len(var_names)}), shapes({len(shapes)}), dtypes({len(dtypes)}), orig_lengths({len(orig_lengths)}), arrays({len(arrays)})"
                )

            with netCDF4.Dataset(output_path, 'w') as ds:
                # 为每个变量的每个维度创建唯一的维度名，避免冲突
                var_dim_names = []
                for i, name in enumerate(var_names):
                    shape = shapes[i]
                    dims = []
                    for j, dim_len in enumerate(shape):
                        dim_name = f"{name}_dim{j}"
                        if dim_name not in ds.dimensions:
                            ds.createDimension(dim_name, dim_len)
                        dims.append(dim_name)
                    var_dim_names.append(tuple(dims))
                # 写变量
                for i, name in enumerate(var_names):
                    shape = shapes[i]
                    dtype = dtypes[i]
                    attrs = var_attrs.get(name, {})
                    fill_value = fill_values.get(name, None)
                    dims = var_dim_names[i]
                    arr = arrays[i]
                    orig_length = orig_lengths[i]
                    valid = arr[:orig_length]
                    # 类型还原
                    np_dtype = np.dtype(dtype)
                    # NaN转为缺测值（仅对整数型）
                    if np.issubdtype(np_dtype, np.integer) and fill_value is not None:
                        valid = np.where(np.isnan(valid), fill_value, valid)
                        valid = valid.astype(np_dtype)
                    else:
                        valid = valid.astype(np_dtype)
                    # 创建变量
                    if fill_value is not None:
                        var = ds.createVariable(name, np_dtype, dims, fill_value=fill_value)
                    else:
                        var = ds.createVariable(name, np_dtype, dims)
                    var[:] = valid.reshape(shape)
                    # 写变量属性
                    for k, v in attrs.items():
                        try:
                            var.setncattr(k, v)
                        except Exception:
                            logger.warning(f"变量 {name} 属性 {k}={v} 写入失败")
                # 写全局属性
                for k, v in global_attrs.items():
                    try:
                        ds.setncattr(k, v)
                    except Exception:
                        logger.warning(f"全局属性 {k}={v} 写入失败")
            logger.info(f"写入 NetCDF 文件到 {output_path}")
        except Exception as e:
            logger.error(f"写入 NetCDF 文件失败: {e}")
            raise