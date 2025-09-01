import numpy as np
import rasterio
from rasterio.transform import from_origin
from tifffile import tifffile

from dfwriter.dfwriter import DfWriter
from parser.tif_parser import TIFParser
from sdk.dacp_client import DacpClient, Principal
from utils.logger_utils import get_logger
logger = get_logger(__name__)


def compare_tiffs_pixel_perfect(file1_path: str, file2_path: str) -> bool:
    """
    比较两个TIFF文件的像素数据和核心结构是否完全一致。

    比较项:
    1. 页面数量
    2. 每一页的形状 (shape)
    3. 每一页的数据类型 (dtype)
    4. 每一页的像素值

    返回:
    - True: 如果所有比较项都一致。
    - False: 如果有任何不一致，并打印出第一个发现的差异点。
    """
    try:
        with tifffile.TiffFile(file1_path) as tif1, tifffile.TiffFile(file2_path) as tif2:
            # 1. 比较页面数量
            if len(tif1.pages) != len(tif2.pages):
                print(f"差异: 页面数量不一致。")
                print(f"  - '{file1_path}': {len(tif1.pages)} 页")
                print(f"  - '{file2_path}': {len(tif2.pages)} 页")
                return False

            # 2. 逐页比较
            for i, (page1, page2) in enumerate(zip(tif1.pages, tif2.pages)):
                # 2a. 比较形状 (shape)
                if page1.shape != page2.shape:
                    print(f"差异: 第 {i + 1} 页的形状不一致。")
                    print(f"  - '{file1_path}': {page1.shape}")
                    print(f"  - '{file2_path}': {page2.shape}")
                    return False

                # 2b. 比较数据类型 (dtype)
                if page1.dtype != page2.dtype:
                    print(f"差异: 第 {i + 1} 页的数据类型不一致。")
                    print(f"  - '{file1_path}': {page1.dtype}")
                    print(f"  - '{file2_path}': {page2.dtype}")
                    return False

                # 2c. 比较像素值
                # asarray() 将页面数据加载到 numpy 数组中
                arr1 = page1.asarray()
                arr2 = page2.asarray()

                # 使用 numpy.array_equal 进行高效且精确的比较
                if not np.array_equal(arr1, arr2):
                    print(f"差异: 第 {i + 1} 页的像素值不一致。")
                    # 可选：打印出差异的详细信息
                    diff_indices = np.where(arr1 != arr2)
                    print(f"  - 在位置 {diff_indices[0][0], ...} 处发现第一个差异值:")
                    print(f"    '{file1_path}': {arr1[diff_indices][0]}")
                    print(f"    '{file2_path}': {arr2[diff_indices][0]}")
                    return False

            print("文件在像素数据和核心结构上完全一致。")
            return True

    except Exception as e:
        print(f"比较过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    parser = TIFParser()
    # table = parser.parse("D:/data/geotiff/Landsat8_Ortho_BestPixel.tif")
    table = parser.parse("/Users/zjcheng/Downloads/file_example_TIFF_1MB.tiff")
    print(table)
    print(table.schema)
    print(table.schema.metadata)
    print(f"行数: {table.num_rows}, 列数: {table.num_columns}")
    print(f"列名: {table.column_names}")
    print(f"数据大小: {table.nbytes} bytes")
    # print(f"打印前5行，每列前3个值:\n {table.to_string(head_rows=5, first_cols=3, display_all=False)}")
    print(f"打印所有数据:\n {table.to_string()}")

    sample_table = parser.sample("/Users/zjcheng/Downloads/file_example_TIFF_1MB.tiff")
    print(sample_table)
    print(sample_table.schema)
    print(sample_table.schema.metadata)
    print(f"行数: {sample_table.num_rows}, 列数: {sample_table.num_columns}")
    print(f"列名: {sample_table.column_names}")
    print(f"数据大小: {sample_table.nbytes} bytes")
    # print(f"打印前5行，每列前3个值:\n {sample_table.to_string(head_rows=5, first_cols=3, display_all=False)}")
    print(f"打印所有数据:\n {sample_table.to_string()}")

    count = parser.count("/Users/zjcheng/Downloads/file_example_TIFF_1MB.tiff")
    print(f"统计行数: {count}")

    dfwriter = DfWriter()
    dfwriter.output("/Users/zjcheng/Downloads/file_example_TIFF_1MB_new.tiff").format("tiff").write_table(table)

    are_they_same = compare_tiffs_pixel_perfect("/Users/zjcheng/Downloads/file_example_TIFF_1MB.tiff", "/Users/zjcheng/Downloads/file_example_TIFF_1MB_new.tiff")
    print(f"比较结果: {'一致' if are_they_same else '不一致'}")
