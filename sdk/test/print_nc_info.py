import xarray as xr
import sys


def logger.info_nc_info(file_path):
    """
    读取 NetCDF 文件，并打印其中的变量名、维度、数据类型和形状。

    参数:
    - file_path (str): NetCDF 文件路径
    """
    with xr.open_dataset(file_path) as ds:
        logger.info("文件加载成功！")
        logger.info("\n全局维度:")
        for dim, size in ds.dims.items():
            logger.info(f"  {dim}: {size}")

        logger.info("\n变量列表:")
        for var_name, var in ds.variables.items():
            logger.info(f"  {var_name} -> 维度: {var.dims}, 数据类型: {var.dtype}, 形状: {var.shape}")


if __name__ == "__main__":

    nc_file = "/Users/zhouziang/Documents/test-data/test.nc"
    logger.info_nc_info(nc_file)
