import os
import logging
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import xarray as xr
import dask
import netCDF4
from parser.abstract_parser import BaseParser

logger = logging.getLogger(__name__)

def get_auto_chunk_size(var_shape, dtype=np.float64, target_mem_mb=10):
    dtype_size = np.dtype(dtype).itemsize
    if len(var_shape) == 0:
        return 1
    other_dim = int(np.prod(var_shape[1:])) if len(var_shape) > 1 else 1
    max_chunk = max(10, int((target_mem_mb * 1024 * 1024) // (other_dim * dtype_size)))
    return max_chunk

def is_large_variable(shape, dtype=np.float64, threshold_mb=50):
    dtype_size = np.dtype(dtype).itemsize
    size_mb = np.prod(shape) * dtype_size / 1024 / 1024
    return size_mb > threshold_mb

class NCParser(BaseParser):
    def parse(self, file_path: str) -> pa.Table:
        """
        用 xarray+dask 流式分块读取超大 NetCDF 文件，避免 OOM。
        并提取 dtype、压缩参数等元信息。
        """
        # DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/tif/")
        DEFAULT_ARROW_CACHE_PATH = os.path.join("D:/faird_cache/dataframe/nc/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        file_size = os.path.getsize(file_path)
        logger.info(f"NetCDF 文件大小: {file_size} bytes")

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
            main_axes = [ds[v].dims[0] if ds[v].ndim > 0 else None for v in var_names]
            main_lens = [ds[v].shape[0] if ds[v].ndim > 0 else 1 for v in var_names]
            var_dims = {v: ds[v].dims for v in var_names}
            # 提取压缩参数
            var_compress = {}
            with netCDF4.Dataset(file_path) as nc:
                file_format = getattr(nc, 'file_format', 'unknown')
                for v in var_names:
                    var = nc.variables[v]
                    compress_info = {}
                    for attr in ['zlib', 'complevel', 'shuffle', 'chunksizes']:
                        if hasattr(var, attr):
                            compress_info[attr] = getattr(var, attr)
                    var_compress[v] = compress_info

            logger.info(f"变量列表: {var_names}")
            logger.info(f"变量 shapes: {shapes}")
            logger.info(f"变量 dtypes: {dtypes}")

            schema = pa.schema([pa.field(v, pa.from_numpy_dtype(ds[v].dtype)) for v in var_names])
            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str(global_attrs),
                "orig_lengths": str(main_lens),
                "var_dims": str(var_dims),
                "file_type": file_format,
                "var_compress": str(var_compress)
            }
            schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})

            large_vars = []
            small_vars = []
            for i, shape in enumerate(shapes):
                if is_large_variable(shape, dtype=np.float64, threshold_mb=50):
                    large_vars.append((i, var_names[i]))
                else:
                    small_vars.append((i, var_names[i]))

            max_chunks = []
            for i, shape in enumerate(shapes):
                chunk = get_auto_chunk_size(shape, dtype=np.float64, target_mem_mb=10)
                max_chunks.append(chunk)
            total_chunks = max([int(np.ceil(main_lens[i] / max_chunks[i])) for i in range(len(var_names))])

            logger.info(f"总分块数: {total_chunks}")

            with ipc.new_file(arrow_file_path, schema) as writer:
                for chunk_idx in range(total_chunks):
                    chunk_arrays = [None] * len(var_names)
                    chunk_lens = [0] * len(var_names)
                    logger.info(f"处理第 {chunk_idx+1}/{total_chunks} 块")
                    # 大变量串行
                    for i, v in large_vars:
                        safe_chunk_size = max_chunks[i]
                        main_dim = main_axes[i]
                        main_len = main_lens[i]
                        start = chunk_idx * safe_chunk_size
                        end = min(start + safe_chunk_size, main_len)
                        logger.debug(f"大变量 {v} 分块: start={start}, end={end}, shape={ds[v].shape}")
                        if start >= end:
                            arr_flat = np.array([], dtype=ds[v].dtype)
                        else:
                            darr = ds[v].isel({main_dim: slice(start, end)}).data
                            arr = darr.compute() if hasattr(darr, "compute") else np.array(darr)
                            arr_flat = np.array(arr).flatten()
                        chunk_arrays[i] = arr_flat
                        chunk_lens[i] = len(arr_flat)
                    # 小变量并行
                    batch_idxs = [i for i, _ in small_vars]
                    dask_chunks = []
                    safe_chunk_sizes = []
                    for i, v in small_vars:
                        safe_chunk_size = max_chunks[i]
                        main_dim = main_axes[i]
                        main_len = main_lens[i]
                        start = chunk_idx * safe_chunk_size
                        end = min(start + safe_chunk_size, main_len)
                        logger.debug(f"小变量 {v} 分块: start={start}, end={end}, shape={ds[v].shape}")
                        if start >= end:
                            dask_chunks.append(np.array([], dtype=ds[v].dtype))
                        else:
                            dask_chunks.append(ds[v].isel({main_dim: slice(start, end)}).data)
                        safe_chunk_sizes.append(safe_chunk_size)
                    computed_chunks = []
                    if dask_chunks:
                        try:
                            computed_chunks = dask.compute(*dask_chunks, scheduler="threads")
                        except Exception as e:
                            logger.warning(f"小变量并行失败，自动降级为串行: {e}")
                            computed_chunks = []
                            for chunk in dask_chunks:
                                computed_chunks.append(chunk.compute() if hasattr(chunk, "compute") else chunk)
                        for idx, arr in zip(batch_idxs, computed_chunks):
                            arr_flat = np.array(arr).flatten()
                            chunk_arrays[idx] = arr_flat
                            chunk_lens[idx] = len(arr_flat)
                    # 统一补齐到本次最大长度
                    max_len_this_chunk = max(chunk_lens) if chunk_lens else 0
                    for i, arr_flat in enumerate(chunk_arrays):
                        dtype = ds[var_names[i]].dtype
                        if arr_flat is None:
                            padded = np.full(max_len_this_chunk, np.nan, dtype=dtype)
                            chunk_arrays[i] = pa.array(padded)
                        elif len(arr_flat) < max_len_this_chunk:
                            padded = np.full(max_len_this_chunk, np.nan, dtype=dtype)
                            padded[:len(arr_flat)] = arr_flat.astype(dtype)
                            chunk_arrays[i] = pa.array(padded)
                        else:
                            chunk_arrays[i] = pa.array(arr_flat.astype(dtype))
                    table = pa.table(chunk_arrays, names=var_names)
                    writer.write_table(table)
            ds.close()
            logger.info(f"Arrow Table 已写入缓存文件: {arrow_file_path}")
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

    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写回 NetCDF 文件。
        支持变量属性、全局属性、缺测值、原始dtype和shape的还原。
        分批写入，避免内存溢出。自动处理 datetime64[ns] 类型为 float64。
        支持还原压缩参数，保证文件大小与原始一致。
        """
        import gc
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
            var_dims = _meta_eval(get_meta(meta, 'var_dims', '{}'), {})
            var_compress = _meta_eval(get_meta(meta, 'var_compress', '{}'), {})

            logger.info(f"写入 NetCDF 文件: {output_path}")
            logger.info(f"变量名: {var_names}")

            if not (len(var_names) == len(shapes) == len(dtypes) == len(orig_lengths)):
                raise ValueError(
                    f"元数据长度不一致: var_names({len(var_names)}), shapes({len(shapes)}), dtypes({len(dtypes)}), orig_lengths({len(orig_lengths)})"
                )

            # 动态估算 batch_size
            def estimate_row_bytes(shapes, dtypes):
                total = 0
                for shape, dtype in zip(shapes, dtypes):
                    n = int(np.prod(shape[1:])) if len(shape) > 1 else 1
                    total += np.dtype(dtype).itemsize * n
                return total

            try:
                import psutil
                avail_mem = psutil.virtual_memory().available
                target_mem = avail_mem // 4
            except Exception:
                target_mem = 512 * 1024 * 1024  # 默认512MB

            row_bytes = estimate_row_bytes(shapes, dtypes)
            batch_size = max(1000, min(100000, target_mem // max(row_bytes, 1)))
            logger.info(f"动态设置 batch_size={batch_size}，单行约{row_bytes}字节，可用内存目标{target_mem}字节")

            with netCDF4.Dataset(output_path, 'w') as ds:
                # 1. 创建所有维度
                for i, name in enumerate(var_names):
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shapes[i]))])
                    shape = shapes[i]
                    for dim_name, dim_len in zip(dims, shape):
                        if dim_name not in ds.dimensions:
                            ds.createDimension(dim_name, dim_len)
                # 2. 创建所有变量
                nc_vars = []
                dtype_map = []
                for i, name in enumerate(var_names):
                    shape = shapes[i]
                    dtype = dtypes[i]
                    fill_value = fill_values.get(name, None)
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shape))])
                    np_dtype = np.dtype(dtype)
                    attrs = var_attrs.get(name, {})
                    # 处理 datetime64[ns] 类型
                    if np.issubdtype(np_dtype, np.datetime64):
                        logger.warning(f"{name}: datetime64[ns] 不被 netCDF4 支持，自动转为 float64（单位：天）")
                        np_dtype = np.float64
                        attrs['units'] = attrs.get('units', 'days since 1970-01-01')
                    dtype_map.append(np_dtype)
                    # 还原压缩参数
                    compress_info = var_compress.get(name, {})
                    create_kwargs = {}
                    for k in ['zlib', 'complevel', 'shuffle', 'chunksizes']:
                        if k in compress_info and compress_info[k] is not None:
                            create_kwargs[k] = compress_info[k]
                    if fill_value is not None:
                        var = ds.createVariable(name, np_dtype, dims, fill_value=fill_value, **create_kwargs)
                    else:
                        var = ds.createVariable(name, np_dtype, dims, **create_kwargs)
                    nc_vars.append(var)
                    var_attrs[name] = attrs  # 更新属性，后续写属性用
                # 3. 分批写入数据
                write_offsets = [0 for _ in var_names]
                batch_count = 0 # 记录批次数
                logger.info(f"开始分批写入数据")
                for batch_idx, batch in enumerate(table.to_batches(batch_size)):
                    batch_count += 1
                    for i, arr in enumerate(batch.columns):
                        arr_np = arr.to_numpy()
                        orig_length = orig_lengths[i]
                        remain = orig_length - write_offsets[i]
                        shape_i = shapes[i]
                        np_dtype = dtype_map[i]
                        other_dim = int(np.prod(shape_i[1:])) if len(shape_i) > 1 else 1
                        max_write_rows = len(arr_np) // other_dim
                        write_len = min(remain, max_write_rows)
                        if write_len <= 0:
                            continue
                        arr_write = arr_np[:write_len * other_dim]
                        # 类型转换和特殊处理
                        if np.issubdtype(np_dtype, np.datetime64):
                            arr_write = arr_write.astype('datetime64[D]').astype('float64')
                        if np.issubdtype(np_dtype, np.integer) and fill_values.get(var_names[i], None) is not None:
                            arr_write = np.where(np.isnan(arr_write), fill_values[var_names[i]], arr_write)
                            arr_write = arr_write.astype(np_dtype)
                        else:
                            arr_write = arr_write.astype(np_dtype)
                        arr_write = arr_write.reshape((write_len,) + tuple(shape_i[1:]))
                        nc_vars[i][write_offsets[i]:write_offsets[i]+write_len, ...] = arr_write
                        write_offsets[i] += write_len
                        del arr_write  # 只在定义后删除
                    del batch, arr_np
                    gc.collect()
                logger.info(f"共写入 {batch_count} 批数据")
                # 4. 写变量属性
                for i, name in enumerate(var_names):
                    attrs = var_attrs.get(name, {})
                    for k, v in attrs.items():
                        if k == "_FillValue":
                            continue
                        try:
                            nc_vars[i].setncattr(k, v)
                        except Exception:
                            logger.warning(f"变量 {name} 属性 {k}={v} 写入失败")
                # 5. 写全局属性
                for k, v in global_attrs.items():
                    try:
                        ds.setncattr(k, v)
                    except Exception:
                        logger.warning(f"全局属性 {k}={v} 写入失败")
            logger.info(f"写入 NetCDF 文件到 {output_path}")
        except Exception as e:
            logger.error(f"写入 NetCDF 文件失败: {e}")
            raise

    def sample(self, file_path: str) -> pa.Table:
        """
        从 NetCDF 文件中采样数据，返回 Arrow Table。
        默认每个变量只读取前10个主轴切片（如 time 维度的前10个）。
        最终所有列补齐为20行（不足补NaN，超出截断），避免 ArrowInvalid 错误。
        并为 schema 添加 metadata。
        """
        try:
            ds = xr.open_dataset(file_path)
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            arrays = []
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
            max_len = 20  # 设置最大长度为20行
            # 用 nan 补齐所有列为20行
            for arr_flat in arr_list:
                dtype = ds[var_names[arr_list.index(arr_flat)]].dtype
                if len(arr_flat) < max_len:
                    padded = np.full(max_len, np.nan, dtype=dtype)
                    padded[:len(arr_flat)] = arr_flat.astype(dtype)
                    arrays.append(pa.array(padded))
                else:
                    arrays.append(pa.array(arr_flat[:max_len].astype(dtype)))
            # 构造 schema 并添加 metadata
            schema = pa.schema([pa.field(v, pa.from_numpy_dtype(ds[v].dtype)) for v in var_names])
            shapes = [tuple(ds[v].shape) for v in var_names]
            dtypes = [str(ds[v].dtype) for v in var_names]
            var_attrs = {v: dict(ds[v].attrs) for v in var_names}
            fill_values = {v: var_attrs[v].get('_FillValue', None) for v in var_names}
            global_attrs = dict(ds.attrs)
            orig_lengths = [int(np.prod(ds[v].shape)) for v in var_names]
            var_dims = {v: ds[v].dims for v in var_names}
            # 提取压缩参数
            var_compress = {}
            with netCDF4.Dataset(file_path) as nc:
                for v in var_names:
                    var = nc.variables[v]
                    compress_info = {}
                    for attr in ['zlib', 'complevel', 'shuffle', 'chunksizes']:
                        if hasattr(var, attr):
                            compress_info[attr] = getattr(var, attr)
                    var_compress[v] = compress_info
            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str(global_attrs),
                "orig_lengths": str(orig_lengths),
                "var_dims": str(var_dims),
                "var_compress": str(var_compress),
                "sample": "True"
            }
            schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})
            table = pa.table(arrays, schema=schema)
            ds.close()
            return table
        except Exception as e:
            logger.error(f"采样 NetCDF 文件失败: {e}")
            raise