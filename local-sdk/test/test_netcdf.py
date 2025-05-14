
import faird
from dataframe import DataFrame


def test_netcdf_file():
    """
    测试 NetCDF 文件的加载和写回功能。
    现在通过 df.write(...) 接口完成，不再依赖 NCParser 实例。
    """

    input_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/test_data.nc"
    output_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/output_test.nc"

    print("🔍 正在加载 DataFrame...")
    df = faird.open(input_path)

    if df is None:
        print("加载失败：faird.open 返回 None。请检查 parser 或文件路径。")
        return

    print("DataFrame 加载成功")
    print(f"Schema: {df.schema}")
    print(f"Columns: {df.column_names}")
    print(f"Number of rows: {df.num_rows}")
    print(f"Memory usage: {df.nbytes} bytes")

    print(f"正在使用 df.write(...) 写回文件到: {output_path}")

    try:
        df.write(output_path=output_path)
        print(f"成功写入文件: {output_path}")
    except Exception as e:
        print(f"写入文件失败: {e}")


if __name__ == "__main__":
    test_netcdf_file()