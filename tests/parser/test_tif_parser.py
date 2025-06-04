import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import numpy as np
import pyarrow as pa
import tifffile
import logging
from parser.tif_parser import TIFParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_tiff(path, shape=(3, 10, 10), dtype=np.uint8):
    """创建一个测试用的TIFF文件"""
    data = np.random.randint(0, 255, size=shape, dtype=dtype)
    tifffile.imwrite(path, data)
    logger.info(f"创建测试TIFF文件: {path}, shape={shape}, dtype={dtype}")
    return data

def test_parse_and_write_multiband(tmp_path):
    tif_path = tmp_path / "test_multi.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(3, 8, 8))
    parser = TIFParser()
    logger.info("测试多波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert isinstance(table, pa.Table)
    assert len(table.columns) == 3
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_multi.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("write 方法通过，验证写出数据与原始数据一致")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_singleband(tmp_path):
    tif_path = tmp_path / "test_single.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(8, 8))
    parser = TIFParser()
    logger.info("测试单波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert len(table.columns) == 1
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("单波段 parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_single.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("单波段写出数据验证")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_hwcbands(tmp_path):
    tif_path = tmp_path / "test_hwc.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(8, 8, 3))
    parser = TIFParser()
    logger.info("测试HWC多波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert len(table.columns) == 3
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("HWC多波段 parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_hwc.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("HWC多波段写出数据验证")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_multipage(tmp_path):
    tif_path = tmp_path / "test_multi_page.tif"
    data1 = np.random.randint(0, 255, size=(8, 8), dtype=np.uint8)
    data2 = np.random.randint(0, 255, size=(3, 6, 6), dtype=np.uint8)
    with tifffile.TiffWriter(str(tif_path)) as tif:
        tif.write(data1)
        tif.write(data2)
    parser = TIFParser()
    logger.info("测试多页TIFF parse 方法")
    table = parser.parse(str(tif_path))
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # # 第一页单波段，第二页三波段
    # assert len(table.columns) == 1 + 3
    # out_tif_path = tmp_path / "out_multi_page.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data1 = tif.pages[0].asarray()
    #     out_data2 = tif.pages[1].asarray()
    # logger.info("多页TIFF写出数据验证")
    # np.testing.assert_array_equal(data1, out_data1)
    # np.testing.assert_array_equal(data2, out_data2)

def test_invalid_shape(tmp_path):
    tif_path = tmp_path / "invalid.tif"
    data = np.random.randint(0, 255, size=(2, 2, 2, 2), dtype=np.uint8)
    tifffile.imwrite(str(tif_path), data)
    parser = TIFParser()
    logger.info("测试高维TIFF parse 方法（应展平）")
    table = parser.parse(str(tif_path))
    # assert len(table.columns) == 1
    # assert table.num_rows == 16
    table_schema = table.schema
    logger.info(f"高维TIFF解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)

def test_real_tif(tif_path):
    # 这里可以放一个实际的TIFF文件路径进行测试
    parser = TIFParser()
    logger.info("测试实际TIFF文件 parse 方法")
    table = parser.parse(str(tif_path))
    assert isinstance(table, pa.Table)
    logger.info(f"实际TIFF解析的表结构: {table.schema}")
    logger.info("------------------------")
    print(table)
    
    
    
if __name__ == "__main__":
    logger.info("手动运行测试")
    # import tempfile
    # with tempfile.TemporaryDirectory() as tmpdir:
    #     from pathlib import Path
    #     tmp_path = Path(tmpdir)
    #     test_parse_and_write_multiband(tmp_path)
    #     test_parse_and_write_singleband(tmp_path)
    #     test_parse_and_write_hwcbands(tmp_path)
    #     test_parse_and_write_multipage(tmp_path)
    #     test_invalid_shape(tmp_path)
    test_real_tif("D:\\test\\faird\\sample.tiff")  # 替换为实际的TIFF文件路径
    logger.info("全部测试完成")