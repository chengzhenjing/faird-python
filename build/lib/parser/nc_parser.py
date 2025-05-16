import os
import netCDF4 as nc
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from parser.abstract_parser import BaseParser


class NCParser(BaseParser):
    """
    NetCDF file parser implementing the BaseParser interface.
    支持多维数组（1D, 2D, 3D），支持 object 类型字段，并可正确读写。
    """

    def parse(self, file_path: str) -> pa.Table:
        """
        Parse a NetCDF (.nc) file into a pyarrow Table.

        Args:
            file_path (str): Path to the input NetCDF file.
        Returns:
            pa.Table: A pyarrow Table object representing the NetCDF data.
        """

        # 设置缓存路径
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/nc/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)

        # 构造缓存文件路径
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # 如果缓存存在，直接从缓存加载
        if os.path.exists(arrow_file_path):
            print(f"🔁 从缓存加载 {arrow_file_path}")
            try:
                with pa.memory_map(arrow_file_path, "r") as source:
                    result = ipc.open_file(source).read_all()
                print("✅ 缓存加载成功，返回 Arrow Table")
                print("   Schema:", result.schema)
                print("   Type of result:", type(result))
                return result
            except Exception as e:
                print(f"🚨 缓存加载失败，将重新解析: {e}")

        # 打开 NetCDF 文件
        print(f"📂 正在解析 NetCDF 文件: {file_path}")
        dataset = nc.Dataset(file_path)

        arrays = []
        names = []

        # 存储一维维度数据（如 time, lat, lon）
        dim_vars = {}

        # 第一次遍历：收集所有一维变量作为维度
        for var_name in dataset.variables:
            variable = dataset.variables[var_name]
            if len(variable.shape) == 1:
                dim_vars[var_name] = variable[:]
                print(f"   📐 维度变量 '{var_name}' 加载完成，长度: {len(dim_vars[var_name])}")

        # 第二次遍历：处理多维变量并构建数组
        for var_name in dataset.variables:
            variable = dataset.variables[var_name]
            data = variable[:]
            dtype = data.dtype.name

            # 跳过已处理的一维维度变量
            if len(data.shape) == 1 and var_name in dim_vars:
                continue

            # 获取变量的维度名
            dim_names = variable.dimensions
            if not dim_names:
                print(f"⚠️ 变量 '{var_name}' 没有维度信息，跳过")
                continue

            # 如果是标量或空变量，跳过
            if len(data.shape) == 0:
                print(f"⚠️ 变量 '{var_name}' 是标量，跳过")
                continue

            # 主维度（通常第一个维度为主时间轴等）
            main_dim_name = dim_names[0]
            main_dim_size = data.shape[0]

            # 构建广播后的坐标网格
            coord_grids = []

            for i, dim_name in enumerate(dim_names):
                if dim_name not in dim_vars:
                    raise ValueError(f"Dimension '{dim_name}' not found in variables")

                dim_data = dim_vars[dim_name]
                expand_shape = [1] * len(dim_names)
                expand_shape[i] = -1
                expanded = np.broadcast_to(dim_data.reshape(expand_shape), data.shape)
                coord_grids.append(expanded.flatten())

            # 添加未添加过的维度列
            for i, dim_name in enumerate(dim_names):
                if dim_name not in names:
                    arrays.append(pa.array(coord_grids[i], type=pa.from_numpy_dtype(coord_grids[i].dtype)))
                    names.append(dim_name)
                    print(f"   ➕ 添加维度列 '{dim_name}', 长度: {coord_grids[i].shape[0]}")

            # 展平数据并添加到数组中
            flat_data = data.flatten()
            if dtype == 'object':
                if isinstance(flat_data[0], str):
                    pa_type = pa.string()
                else:
                    pa_type = pa.binary()
                array = pa.array(flat_data, type=pa_type)
            else:
                pa_type_class = getattr(pa, dtype, None)
                if pa_type_class is None:
                    raise ValueError(f"Unsupported data type: {dtype}")
                array = pa.array(flat_data, type=pa_type_class())

            arrays.append(array)
            names.append(var_name)
            print(f"   ➕ 添加变量 '{var_name}', 数据长度: {flat_data.shape[0]}")

        # 创建 Arrow 表格
        print("📊 开始构建 Arrow Table...")
        try:
            table = pa.Table.from_arrays(arrays, names=names)
            print("✅ Arrow Table 构建成功")
            print("   Schema:", table.schema)
            print("   Number of rows:", table.num_rows)
        except pa.lib.ArrowInvalid as e:
            print("❌ 构建 Arrow Table 失败: 列长度不一致")
            for i, arr in enumerate(arrays):
                print(f"   Column '{names[i]}': length={len(arr)}")
            raise

        # 写入缓存
        print(f"💾 写入缓存文件: {arrow_file_path}")
        try:
            with ipc.new_file(arrow_file_path, table.schema) as writer:
                writer.write_table(table)
            print("   ✅ 缓存写入成功")
        except Exception as e:
            print(f"   ❌ 缓存写入失败: {e}")
            raise

        # 零拷贝读取返回
        print("🔁 从缓存中加载 Arrow Table 返回")
        try:
            with pa.memory_map(arrow_file_path, "r") as source:
                result = ipc.open_file(source).read_all()
            print("✅ 成功加载缓存中的 Arrow Table")
            print("   Schema:", result.schema)
            print("   Type of result:", type(result))
            return result
        except Exception as e:
            print(f"🚨 加载缓存失败: {e}")
            raise

    def write(self, table: pa.Table, output_path: str):
        """
        将 pyarrow.Table 写回 NetCDF 文件，支持多维数组。

        Args:
            table (pa.Table): 要写入的数据。
            output_path (str): 输出文件路径。
        """
        with nc.Dataset(output_path, 'w', format='NETCDF4') as dataset:
            # 创建主维度（行数）
            row_dim = dataset.createDimension('row', len(table))

            # 遍历每一列
            for col_name in table.column_names:
                col_array = table[col_name]

                # 判断是否为 FixedSizeListArray（即二维或三维结构）
                if pa.types.is_fixed_size_list(col_array.type):
                    inner_size = col_array.type.list_size
                    value_type = col_array.type.value_type.to_pandas_dtype()

                    # 处理二维结构
                    if pa.types.is_fixed_size_list(col_array.values.type):
                        outer_size = col_array.values.type.list_size
                        total_dim = dataset.createDimension(f"{col_name}_dim", inner_size * outer_size)
                        var = dataset.createVariable(col_name, value_type, ('row', total_dim))
                        values = col_array.flatten().to_numpy().reshape(-1, outer_size * inner_size)
                        var[:, :] = values

                    # 处理一维嵌套结构
                    else:
                        inner_dim_name = f"{col_name}_dim"
                        inner_dim = dataset.createDimension(inner_dim_name, inner_size)
                        var = dataset.createVariable(col_name, value_type, ('row', inner_dim_name))
                        values = col_array.flatten().to_numpy().reshape(-1, inner_size)
                        var[:, :] = values

                elif pa.types.is_string(col_array.type) or pa.types.is_binary(col_array.type):
                    # 字符串或二进制类型（来自原始 object 类型）
                    var = dataset.createVariable(col_name, str, ('row',))
                    var[:] = col_array.to_numpy().astype(str)

                elif pa.types.is_primitive(col_array.type):
                    # 一维基本类型数组
                    var_type = col_array.type.to_pandas_dtype()
                    var = dataset.createVariable(col_name, var_type, ('row',))
                    var[:] = col_array.to_numpy()

                else:
                    raise ValueError(f"Unsupported array type: {col_array.type}")
