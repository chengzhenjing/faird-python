# import os
# import logging
# import numpy as np
# import pyarrow as pa
# import pyarrow.ipc as ipc
# import xarray as xr
# import dask
# import netCDF4
# from parser.abstract_parser import BaseParser

# logger = logging.getLogger(__name__)

# def get_auto_chunk_size(var_shape, dtype=np.float64, target_mem_mb=100):
#     dtype_size = np.dtype(dtype).itemsize
#     if len(var_shape) == 0:
#         return 1
#     other_dim = int(np.prod(var_shape[1:])) if len(var_shape) > 1 else 1
#     max_chunk = max(100, int((target_mem_mb * 1024 * 1024) // (other_dim * dtype_size)))
#     return max_chunk

# def is_large_variable(shape, dtype=np.float64, threshold_mb=50):
#     dtype_size = np.dtype(dtype).itemsize
#     size_mb = np.prod(shape) * dtype_size / 1024 / 1024
#     return size_mb > threshold_mb

# class NCParser(BaseParser):
#     def parse(self, file_path: str) -> pa.Table:
#         """
#         用 xarray+dask 流式分块读取超大 NetCDF 文件，避免 OOM。
#         大变量串行分块，小变量小批量并行，提升效率。
#         """
#         DEFAULT_ARROW_CACHE_PATH = os.path.join("D:/faird_cache/dataframe/nc/")
#         os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
#         arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
#         arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

#         file_size = os.path.getsize(file_path)
#         logger.info(f"NetCDF 文件大小: {file_size} bytes")

#         try:
#             if os.path.exists(arrow_file_path):
#                 logger.info(f"检测到缓存文件，直接从 {arrow_file_path} 读取 Arrow Table。")
#                 with pa.memory_map(arrow_file_path, "r") as source:
#                     return ipc.open_file(source).read_all()
#         except Exception as e:
#             logger.error(f"读取缓存 .arrow 文件失败: {e}")

#         try:
#             logger.info(f"开始用 xarray+dask 读取 NetCDF 文件: {file_path}")
#             ds = xr.open_dataset(file_path, chunks={})
#             var_names = [v for v in ds.variables if ds[v].ndim > 0]
#             shapes = [tuple(ds[v].shape) for v in var_names]
#             dtypes = [str(ds[v].dtype) for v in var_names]
#             var_attrs = {v: dict(ds[v].attrs) for v in var_names]
#             fill_values = {v: var_attrs[v].get('_FillValue', None) for v in var_names]
#             global_attrs = dict(ds.attrs)
#             # 只对主轴分块，主轴长度
#             main_axes = [ds[v].dims[0] if ds[v].ndim > 0 else None for v in var_names]
#             main_lens = [ds[v].shape[0] if ds[v].ndim > 0 else 1 for v in var_names]
#             var_dims = {v: ds[v].dims for v in var_names}

#             # 构造schema
#             schema = pa.schema([pa.field(v, pa.float64()) for v in var_names])
#             with netCDF4.Dataset(file_path) as nc:
#                 file_format = getattr(nc, 'file_format', 'unknown')
#             meta = {
#                 "shapes": str(shapes),
#                 "dtypes": str(dtypes),
#                 "var_names": str(var_names),
#                 "var_attrs": str(var_attrs),
#                 "fill_values": str(fill_values),
#                 "global_attrs": str(global_attrs),
#                 "orig_lengths": str(main_lens),
#                 "var_dims": str(var_dims),
#                 "file_type": file_format
#             }
#             schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})

#             # 变量分组
#             large_vars = []
#             small_vars = []
#             for i, shape in enumerate(shapes):
#                 if is_large_variable(shape, dtype=np.float64, threshold_mb=50):
#                     large_vars.append((i, var_names[i]))
#                 else:
#                     small_vars.append((i, var_names[i]))

#             # 计算每个变量的chunk_size
#             max_chunks = []
#             for i, shape in enumerate(shapes):
#                 chunk = get_auto_chunk_size(shape, dtype=np.float64, target_mem_mb=50)
#                 max_chunks.append(chunk)
#             total_chunks = max([int(np.ceil(main_lens[i] / max_chunks[i])) for i in range(len(var_names))])

#             with ipc.new_file(arrow_file_path, schema) as writer:
#                 for chunk_idx in range(total_chunks):
#                     chunk_arrays = [None] * len(var_names)
#                     chunk_lens = [0] * len(var_names)
#                     # 大变量串行
#                     for i, v in large_vars:
#                         safe_chunk_size = max_chunks[i]
#                         main_dim = main_axes[i]
#                         main_len = main_lens[i]
#                         start = chunk_idx * safe_chunk_size
#                         end = min(start + safe_chunk_size, main_len)
#                         if start >= end:
#                             arr_flat = np.array([])
#                         else:
#                             chunk = ds[v].isel({main_dim: slice(start, end)}).values
#                             arr_flat = np.array(chunk).flatten()
#                         chunk_arrays[i] = arr_flat
#                         chunk_lens[i] = len(arr_flat)
#                     # 小变量并行
#                     batch_idxs = [i for i, _ in small_vars]
#                     dask_chunks = []
#                     safe_chunk_sizes = []
#                     for i, v in small_vars:
#                         safe_chunk_size = max_chunks[i]
#                         main_dim = main_axes[i]
#                         main_len = main_lens[i]
#                         start = chunk_idx * safe_chunk_size
#                         end = min(start + safe_chunk_size, main_len)
#                         if start >= end:
#                             dask_chunks.append(np.array([]))
#                         else:
#                             dask_chunks.append(ds[v].isel({main_dim: slice(start, end)}).data)
#                         safe_chunk_sizes.append(safe_chunk_size)
#                     computed_chunks = []
#                     if dask_chunks:
#                         try:
#                             computed_chunks = dask.compute(*dask_chunks, scheduler="threads")
#                         except Exception as e:
#                             logger.warning(f"小变量并行失败，自动降级为串行: {e}")
#                             computed_chunks = []
#                             for chunk in dask_chunks:
#                                 computed_chunks.append(chunk.compute() if hasattr(chunk, "compute") else chunk)
#                         for idx, arr in zip(batch_idxs, computed_chunks):
#                             arr_flat = np.array(arr).flatten()
#                             chunk_arrays[idx] = arr_flat
#                             chunk_lens[idx] = len(arr_flat)
#                     # 统一补齐到本次最大长度
#                     max_len_this_chunk = max(chunk_lens) if chunk_lens else 0
#                     for i, arr_flat in enumerate(chunk_arrays):
#                         if arr_flat is None:
#                             padded = np.full(max_len_this_chunk, np.nan, dtype=np.float64)
#                             chunk_arrays[i] = pa.array(padded)
#                         elif len(arr_flat) < max_len_this_chunk:
#                             padded = np.full(max_len_this_chunk, np.nan, dtype=np.float64)
#                             padded[:len(arr_flat)] = arr_flat.astype(np.float64)
#                             chunk_arrays[i] = pa.array(padded)
#                         else:
#                             chunk_arrays[i] = pa.array(arr_flat.astype(np.float64))
#                     table = pa.table(chunk_arrays, names=var_names)
#                     writer.write_table(table)
#             ds.close()
#         except Exception as e:
#             logger.error(f"解析 NetCDF 文件失败: {e}")
#             raise

#         try:
#             logger.info(f"从 .arrow 文件 {arrow_file_path} 读取 Arrow Table。")
#             with pa.memory_map(arrow_file_path, "r") as source:
#                 return ipc.open_file(source).read_all()
#         except Exception as e:
#             logger.error(f"读取 .arrow 文件失败: {e}")
#             raise

#     def write(self, table: pa.Table, output_path: str):
#         """
#         将 Arrow Table 写回 NetCDF 文件。
#         支持变量属性、全局属性、缺测值、原始dtype和shape的还原。
#         """
#         try:
#             meta = table.schema.metadata or {}

#             def _meta_eval(val, default):
#                 if isinstance(val, bytes):
#                     return eval(val.decode())
#                 elif isinstance(val, str):
#                     return eval(val)
#                 else:
#                     return default

#             def get_meta(meta, key, default):
#                 if key in meta:
#                     return meta[key]
#                 if isinstance(key, str) and key.encode() in meta:
#                     return meta[key.encode()]
#                 if isinstance(key, bytes) and key.decode() in meta:
#                     return meta[key.decode()]
#                 return default

#             shapes = _meta_eval(get_meta(meta, 'shapes', '[]'), [])
#             dtypes = _meta_eval(get_meta(meta, 'dtypes', '[]'), [])
#             var_names = _meta_eval(get_meta(meta, 'var_names', '[]'), [])
#             var_attrs = _meta_eval(get_meta(meta, 'var_attrs', '{}'), {})
#             fill_values = _meta_eval(get_meta(meta, 'fill_values', '{}'), {})
#             global_attrs = _meta_eval(get_meta(meta, 'global_attrs', '{}'), {})
#             orig_lengths = _meta_eval(get_meta(meta, 'orig_lengths', '[]'), [])
#             var_dims = _meta_eval(get_meta(meta, 'var_dims', '{}'), {})
#             arrays = [col.to_numpy() for col in table.columns]

#             # 检查长度一致性
#             if not (len(var_names) == len(shapes) == len(dtypes) == len(orig_lengths) == len(arrays)):
#                 raise ValueError(
#                     f"元数据长度不一致: var_names({len(var_names)}), shapes({len(shapes)}), dtypes({len(dtypes)}), orig_lengths({len(orig_lengths)}), arrays({len(arrays)})"
#                 )

#             with netCDF4.Dataset(output_path, 'w') as ds:
#                 # 先创建所有需要的维度（用原始维度名）
#                 for i, name in enumerate(var_names):
#                     dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shapes[i]))])
#                     shape = shapes[i]
#                     for dim_name, dim_len in zip(dims, shape):
#                         if dim_name not in ds.dimensions:
#                             ds.createDimension(dim_name, dim_len)
#                 # 写变量
#                 for i, name in enumerate(var_names):
#                     shape = shapes[i]
#                     dtype = dtypes[i]
#                     attrs = var_attrs.get(name, {})
#                     fill_value = fill_values.get(name, None)
#                     dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shape))])
#                     arr = arrays[i]
#                     orig_length = orig_lengths[i]
#                     valid = arr[:orig_length]
#                     # 新增：如果数据为空，自动填充缺测值
#                     if valid.size == 0:
#                         logger.warning(f"{name}: 数据为空，自动填充缺测值")
#                         fill = fill_value if fill_value is not None else np.nan
#                         valid = np.full(np.prod(shape), fill, dtype=np.float64)
#                     # 新增：datetime64自动转为float64（单位：天）
#                     np_dtype = np.dtype(dtype)
#                     if np.issubdtype(np_dtype, np.datetime64):
#                         logger.warning(f"{name}: datetime64[ns] 不被 netCDF4 支持，自动转为 float64（单位：天）")
#                         valid = valid.astype('datetime64[D]').astype('float64')
#                         np_dtype = np.float64
#                         attrs['units'] = attrs.get('units', 'days since 1970-01-01')
#                     # 类型还原
#                     if np.issubdtype(np_dtype, np.integer) and fill_value is not None:
#                         valid = np.where(np.isnan(valid), fill_value, valid)
#                         valid = valid.astype(np_dtype)
#                     else:
#                         valid = valid.astype(np_dtype)
#                     # 创建变量
#                     if fill_value is not None:
#                         var = ds.createVariable(name, np_dtype, dims, fill_value=fill_value)
#                     else:
#                         var = ds.createVariable(name, np_dtype, dims)
#                     var[:] = valid.reshape(shape)
#                     # 写变量属性
#                     for k, v in attrs.items():
#                         if k == "_FillValue":
#                             continue  # _FillValue 只能在创建变量时设置
#                         try:
#                             var.setncattr(k, v)
#                         except Exception:
#                             logger.warning(f"变量 {name} 属性 {k}={v} 写入失败")
#                 # 写全局属性
#                 for k, v in global_attrs.items():
#                     try:
#                         ds.setncattr(k, v)
#                     except Exception:
#                         logger.warning(f"全局属性 {k}={v} 写入失败")
#             logger.info(f"写入 NetCDF 文件到 {output_path}")
#         except Exception as e:
#             logger.error(f"写入 NetCDF 文件失败: {e}")
#             raise

#     def sample(self, file_path: str) -> pa.Table:
#         """
#         从 NetCDF 文件中采样数据，返回 Arrow Table。
#         默认每个变量只读取前10个主轴切片（如 time 维度的前10个）。
#         用 nan 补齐所有列为相同长度，避免 ArrowInvalid 错误。
#         并为 schema 添加 metadata。
#         """
#         try:
#             ds = xr.open_dataset(file_path)
#             var_names = [v for v in ds.variables if ds[v].ndim > 0]
#             arrays = []
#             max_len = 0
#             arr_list = []
#             # 先采样并记录每列长度
#             for v in var_names:
#                 var = ds[v]
#                 if var.shape[0] > 10:
#                     arr = var.isel({var.dims[0]: slice(0, 10)}).values
#                 else:
#                     arr = var.values
#                 arr_flat = np.array(arr).flatten()
#                 arr_list.append(arr_flat)
#                 if len(arr_flat) > max_len:
#                     max_len = len(arr_flat)
#             # 用 nan 补齐所有列
#             for arr_flat in arr_list:
#                 if len(arr_flat) < max_len:
#                     padded = np.full(max_len, np.nan, dtype=np.float64)
#                     padded[:len(arr_flat)] = arr_flat.astype(np.float64)
#                     arrays.append(pa.array(padded))
#                 else:
#                     arrays.append(pa.array(arr_flat.astype(np.float64)))
#             # 构造 schema 并添加 metadata
#             schema = pa.schema([pa.field(v, pa.float64()) for v in var_names])
#             shapes = [tuple(ds[v].shape) for v in var_names]
#             dtypes = [str(ds[v].dtype) for v in var_names]
#             var_attrs = {v: dict(ds[v].attrs) for v in var_names]
#             fill_values = {v: var_attrs[v].get('_FillValue', None) for v in var_names]
#             global_attrs = dict(ds.attrs)
#             orig_lengths = [ds[v].shape[0] if ds[v].ndim > 0 else 1 for v in var_names]
#             var_dims = {v: ds[v].dims for v in var_names]
#             with netCDF4.Dataset(file_path) as nc:
#                 file_format = getattr(nc, 'file_format', 'unknown')
#             meta = {
#                 "shapes": str(shapes),
#                 "dtypes": str(dtypes),
#                 "var_names": str(var_names),
#                 "var_attrs": str(var_attrs),
#                 "fill_values": str(fill_values),
#                 "global_attrs": str(global_attrs),
#                 "orig_lengths": str(orig_lengths),
#                 "var_dims": str(var_dims),
#                 "sample": "True",
#                 "file_type": file_format
#             }
#             schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})
#             table = pa.table(arrays, schema=schema)
#             ds.close()
#             return table
#         except Exception as e:
#             logger.error(f"采样 NetCDF 文件失败: {e}")
#             raise