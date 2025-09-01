import pyarrow as pa
import pyarrow.ipc as ipc
import os
import numpy as np
import netCDF4

from dfwriter.abstract_writer import BaseWriter
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class NCWriter(BaseWriter):
    """
    netcdf file writer implementing the BaseWriter interface.
    """
    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写回 NetCDF 文件。大文件非常慢。
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
                    logger.info(f"处理第 {batch_idx + 1} 批数据")
                    batch_count += 1
                    for i, arr in enumerate(batch.columns):
                        arr_np = arr.to_numpy(zero_copy_only=False)
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
                        # 兼容 parse/sample 阶段已转 float 的 cftime.datetime
                        # 如果 units 属性是 days since 1970-01-01，直接写 float64
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