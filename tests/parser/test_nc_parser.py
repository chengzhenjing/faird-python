import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import numpy as np
import pyarrow as pa
import netCDF4
import tempfile
import logging
from parser.nc_parser import NCParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_nc(path):
    """创建一个包含多变量、多维度、属性、缺测值、不同类型的 NetCDF 文件"""
    with netCDF4.Dataset(path, 'w') as ds:
        ds.createDimension('x', 4)
        ds.createDimension('y', 3)
        ds.createDimension('z', 2)
        ds.createDimension('t', 5)
        # 全局属性
        ds.description = "测试NC文件"
        ds.history = "created for unit test"
        # 变量1：2D float32
        v1 = ds.createVariable('var1', 'f4', ('x', 'y'))
        v1[:, :] = np.arange(12, dtype=np.float32).reshape(4, 3)
        v1.units = 'meters'
        v1.long_name = '二维浮点变量'
        # 变量2：3D int16，带缺测值
        v2 = ds.createVariable('var2', 'i2', ('z', 'x', 'y'), fill_value=-9999)
        arr2 = np.arange(24, dtype=np.int16).reshape(2, 4, 3)
        arr2[0, 0, 0] = -9999
        v2[:, :, :] = arr2
        v2.long_name = '三维整型变量'
        # 变量3：1D float64
        v3 = ds.createVariable('var3', 'f8', ('x',))
        v3[:] = np.linspace(0, 1, 4)
        v3.note = '一维变量'
        # 变量4：4D uint8
        v4 = ds.createVariable('var4', 'u1', ('t', 'z', 'x', 'y'))
        arr4 = np.arange(120, dtype=np.uint8).reshape(5, 2, 4, 3)
        v4[:, :, :, :] = arr4
        v4.comment = '四维无符号整型'
    logger.info(f"创建测试NC文件: {path}")

def test_nc_parser(tmp_path):
    nc_path = tmp_path / "test.nc"
    create_test_nc(str(nc_path))
    parser = NCParser()
    logger.info("测试 parse 方法")
    table = parser.parse(str(nc_path))
    assert isinstance(table, pa.Table)
    logger.info(f"Arrow Table 列: {table.column_names}")
    logger.info(f"Arrow Table 行数: {table.num_rows}")
    logger.info(f"Arrow Table schema: {table.schema}")
    print(table)
    # 测试 write 方法
    out_nc_path = tmp_path / "out.nc"
    parser.write(table, str(out_nc_path))
    # 验证写回的NC文件内容
    with netCDF4.Dataset(str(out_nc_path), 'r') as ds:
        # 检查变量
        assert 'var1' in ds.variables
        assert 'var2' in ds.variables
        assert 'var3' in ds.variables
        assert 'var4' in ds.variables
        # 检查数据一致性
        np.testing.assert_array_equal(ds.variables['var1'][:], np.arange(12, dtype=np.float32).reshape(4, 3))
        arr2 = np.arange(24, dtype=np.int16).reshape(2, 4, 3)
        arr2[0, 0, 0] = -9999
        np.testing.assert_array_equal(ds.variables['var2'][:], arr2)
        np.testing.assert_array_equal(ds.variables['var3'][:], np.linspace(0, 1, 4))
        arr4 = np.arange(120, dtype=np.uint8).reshape(5, 2, 4, 3)
        np.testing.assert_array_equal(ds.variables['var4'][:], arr4)
        # 检查全局属性
        assert ds.description == "测试NC文件"
        assert ds.history == "created for unit test"
        # 检查变量属性
        assert ds.variables['var1'].units == 'meters'
        assert ds.variables['var1'].long_name == '二维浮点变量'
        assert ds.variables['var2'].long_name == '三维整型变量'
        assert ds.variables['var3'].note == '一维变量'
        assert ds.variables['var4'].comment == '四维无符号整型'
        # 检查缺测值
        assert hasattr(ds.variables['var2'], '_FillValue')
        assert ds.variables['var2']._FillValue == -9999
    logger.info("parse 和 write 方法测试通过")

def test_nc_parser_real_file(nc_file_path, tmp_path, out_nc_path):
    """
    测试本地真实NC文件的parse和write功能。
    :param nc_file_path: 本地真实nc文件路径
    :param tmp_path: pytest临时目录
    """
    parser = NCParser()
    logger.info(f"测试真实NC文件 parse 方法: {nc_file_path}")
    table = parser.parse(str(nc_file_path))
    assert isinstance(table, pa.Table)
    logger.info(f"Arrow Table 列: {table.column_names}")
    logger.info(f"Arrow Table 行数: {table.num_rows}")
    logger.info(f"Arrow Table schema: {table.schema}")
    print(table)
    # 测试 write 方法
    out_nc_path = tmp_path / out_nc_path
    parser.write(table, str(out_nc_path))
    # 验证写回的NC文件内容是否能正常打开
    with netCDF4.Dataset(str(out_nc_path), 'r') as ds:
        logger.info(f"写回NC文件变量: {list(ds.variables.keys())}")
        logger.info(f"写回NC文件全局属性: {ds.ncattrs()}")
        assert len(ds.variables) > 0
    logger.info("真实NC文件 parse 和 write 方法测试通过")


# 测试函数入口
if __name__ == "__main__":
    import pathlib
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
        # test_nc_parser(tmp_path)
        # 替换为你的真实nc文件路径
        logger.info("开始测试真实NC文件,path [%s]", r"D:\test\faird\test_data.nc")
        test_nc_parser_real_file(r"D:\test\faird\test_data.nc", tmp_path,"test_data_write.nc")
        logger.info("开始测试真实NC文件,path [%s]", r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_qrtrdeg_gridded_coast_monthly.nc")
        test_nc_parser_real_file(r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_qrtrdeg_gridded_coast_monthly.nc", tmp_path,"SOCATv2021_qrtrdeg_gridded_coast_monthly_write.nc")
        logger.info("开始测试真实NC文件,path [%s]", r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_decadal.nc")
        test_nc_parser_real_file(r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_decadal.nc", tmp_path,"SOCATv2021_tracks_gridded_decadal_write.nc")
        logger.info("开始测试真实NC文件,path [%s]", r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_monthly.nc")
        test_nc_parser_real_file(r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_monthly.nc", tmp_path,"SOCATv2021_tracks_gridded_monthly_write.nc")
        logger.info("开始测试真实NC文件,path [%s]", r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_yearly.nc")
        test_nc_parser_real_file(r"D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_tracks_gridded_yearly.nc", tmp_path,"SOCATv2021_tracks_gridded_yearly_write.nc")
        logger.info("所有测试通过")
        