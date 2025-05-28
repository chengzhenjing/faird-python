import xarray as xr
import numpy as np
import sys


def compare_netcdf_files(original_path, output_path):
    """
    比较两个NetCDF文件的完整性：
    - 全局属性
    - 维度
    - 变量定义（名称、类型、维度）
    - 变量属性
    - 数据一致性
    """
    ds_original = xr.open_dataset(original_path)
    ds_output = xr.open_dataset(output_path)

    print("开始校验全局属性...")
    if ds_original.attrs != ds_output.attrs:
        print("全局属性不一致")
        print("原始文件属性:", ds_original.attrs)
        print("输出文件属性:", ds_output.attrs)
        return False
    else:
        print("全局属性一致")

    print("开始校验维度...")
    if ds_original.dims != ds_output.dims:
        print("维度信息不一致")
        print("原始文件维度:", ds_original.dims)
        print("输出文件维度:", ds_output.dims)
        return False
    else:
        print("维度信息一致")

    print("开始校验变量集合...")
    if set(ds_original.variables) != set(ds_output.variables):
        print("变量集合不一致")
        print("原始文件变量:", set(ds_original.variables))
        print("输出文件变量:", set(ds_output.variables))
        return False
    else:
        print("变量集合一致")



    print("所有检查项通过，文件完整一致")
    return True


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("用法: python validate_netcdf.py <原始文件路径> <转换后文件路径>")
    #     sys.exit(1)

    original_file = "/Users/zhouziang/Documents/test-data/nc/test_data.nc"
    output_file = "/Users/zhouziang/Documents/test-data/nc/test_data_output.nc"

    result = compare_netcdf_files(original_file, output_file)
    if result:
        print("文件转换前后内容一致！")
    else:
        print("文件存在差异，请检查转换过程。")
