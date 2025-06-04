import pyarrow as pa
import pyarrow.ipc as ipc
import os
from parser.abstract_parser import BaseParser
import logging
import numpy as np
import xarray as xr
import dask.array as da
import netCDF4

logger = logging.getLogger(__name__)

class NCParser(BaseParser):
    def parse(self, file_path: str) -> pa.Table:
        """
        用 xarray+dask 流式分块读取超大 NetCDF 文件，避免 OOM。
        返回合并后的 pa.Table。
        """
        # DEFAULT_ARROW_CACHE_PATH = os.path.join("D:/faird_cache/dataframe/nc/")
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/csv/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # 根据文件大小动态设置 chunk_size
        file_size = os.path.getsize(file_path)
        logger.info(f"NetCDF 文件大小: {file_size} bytes")
        if file_size < 100 * 1024 * 1024:
            chunk_size = 100_000
        elif file_size < 1 * 1024 * 1024 * 1024:
            chunk_size = 50_000
        else:
            chunk_size = 10_000

        try:
            if os.path.exists(arrow_file_path):
                logger.info(f"检测到缓存文件，直接从 {arrow_file_path} 读取 Arrow Table。")
                with pa.memory_map(arrow_file_path, "r") as source:
                    return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取缓存 .arrow 文件失败: {e}")

        try:
            logger.info(f"开始用 xarray+dask 读取 NetCDF 文件: {file_path}")
            ds = xr.open_dataset(file_path, chunks={})
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            shapes = [tuple(ds[v].shape) for v in var_names]
            dtypes = [str(ds[v].dtype) for v in var_names]
            var_attrs = {v: dict(ds[v].attrs) for v in var_names}
            fill_values = {v: var_attrs[v].get('_FillValue', None) for v in var_names}
            global_attrs = dict(ds.attrs)
            orig_lengths = [int(np.prod(ds[v].shape)) for v in var_names]
            max_len = max(orig_lengths)
            var_dims = {v: ds[v].dims for v in var_names}  # 记录原始维度名

            # 构造schema
            schema = pa.schema([pa.field(v, pa.float64()) for v in var_names])
            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str(global_attrs),
                "orig_lengths": str(orig_lengths),
                "var_dims": str(var_dims)
            }
            schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})

            # 分块流式写入.arrow文件
            with ipc.new_file(arrow_file_path, schema) as writer:
                for start in range(0, max_len, chunk_size):
                    chunk_arrays = []
                    for i, v in enumerate(var_names):
                        darr = ds[v].data  # 可能是 dask array 也可能是 numpy.ndarray
                        arr_flat = darr.reshape(-1)
                        if hasattr(arr_flat, "compute"):
                            arr_chunk = arr_flat[start:start+chunk_size].compute()
                        else:
                            arr_chunk = arr_flat[start:start+chunk_size]
                        # 补齐
                        if len(arr_chunk) < chunk_size:
                            padded = np.full(chunk_size, np.nan, dtype=np.float64)
                            padded[:len(arr_chunk)] = arr_chunk.astype(np.float64)
                            chunk_arrays.append(pa.array(padded))
                        else:
                            chunk_arrays.append(pa.array(arr_chunk.astype(np.float64)))
                    table = pa.table(chunk_arrays, names=var_names)
                    writer.write_table(table)
            ds.close()
        except Exception as e:
            logger.error(f"解析 NetCDF 文件失败: {e}")
            raise

        try:
            logger.info(f"从 .arrow 文件 {arrow_file_path} 读取 Arrow Table。")
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取 .arrow 文件失败: {e}")
            raise
        
    def sample(self, file_path: str) -> pa.Table:
        """
        从 NetCDF 文件中采样数据，返回 Arrow Table。
        默认每个变量只读取前10个主轴切片（如 time 维度的前10个）。
        用 nan 补齐所有列为相同长度，避免 ArrowInvalid 错误。
        """
        try:
            ds = xr.open_dataset(file_path)
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            arrays = []
            col_names = []
            max_len = 0
            arr_list = []
            # 先采样并记录每列长度
            for v in var_names:
                var = ds[v]
                if var.shape[0] > 10:
                    arr = var.isel({var.dims[0]: slice(0, 10)}).values
                else:
                    arr = var.values
                arr_flat = np.array(arr).flatten()
                arr_list.append(arr_flat)
                if len(arr_flat) > max_len:
                    max_len = len(arr_flat)
            # 用 nan 补齐所有列
            for arr_flat in arr_list:
                if len(arr_flat) < max_len:
                    padded = np.full(max_len, np.nan, dtype=np.float64)
                    padded[:len(arr_flat)] = arr_flat.astype(np.float64)
                    arrays.append(pa.array(padded))
                else:
                    arrays.append(pa.array(arr_flat.astype(np.float64)))
            table = pa.table(arrays, names=var_names)
            ds.close()
            return table
        except Exception as e:
            logger.error(f"采样 NetCDF 文件失败: {e}")
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
            var_dims = _meta_eval(get_meta(meta, 'var_dims', '{}'), {})  # 读取原始维度名
            arrays = [col.to_numpy() for col in table.columns]

            # 检查长度一致性
            if not (len(var_names) == len(shapes) == len(dtypes) == len(orig_lengths) == len(arrays)):
                raise ValueError(
                    f"元数据长度不一致: var_names({len(var_names)}), shapes({len(shapes)}), dtypes({len(dtypes)}), orig_lengths({len(orig_lengths)}), arrays({len(arrays)})"
                )

            with netCDF4.Dataset(output_path, 'w') as ds:
                # 先创建所有需要的维度（用原始维度名）
                dims_created = set()
                for i, name in enumerate(var_names):
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shapes[i]))])
                    shape = shapes[i]
                    for dim_name, dim_len in zip(dims, shape):
                        if dim_name not in ds.dimensions:
                            ds.createDimension(dim_name, dim_len)
                        dims_created.add(dim_name)
                # 写变量
                for i, name in enumerate(var_names):
                    shape = shapes[i]
                    dtype = dtypes[i]
                    attrs = var_attrs.get(name, {})
                    fill_value = fill_values.get(name, None)
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shape))])
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
                        if k == "_FillValue":
                            continue  # _FillValue 只能在创建变量时设置
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