import xarray as xr
import sys


def print_nc_info(file_path):
    """
    读取 NetCDF 文件，并打印其中的变量名、维度、数据类型和形状。

    参数:
    - file_path (str): NetCDF 文件路径
    """
    with xr.open_dataset(file_path) as ds:
        print("文件加载成功！")
        print("\n全局维度:")
        for dim, size in ds.dims.items():
            print(f"  {dim}: {size}")

        print("\n变量列表:")
        for var_name, var in ds.variables.items():
            print(f"  {var_name} -> 维度: {var.dims}, 数据类型: {var.dtype}, 形状: {var.shape}")


if __name__ == "__main__":

    nc_file = "/Users/zhouziang/Documents/test-data/test.nc"
    print_nc_info(nc_file)
