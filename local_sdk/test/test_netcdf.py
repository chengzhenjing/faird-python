
import faird
import  pyarrow.compute as pc
from sdk.dacp_client import DacpClient, Principal


SERVER_URL = "dacp://localhost:3101"
USERNAME = "user1@cnic.cn"
TENANT = "conet"
CLIENT_ID = "faird-user1"


def test_netcdf_file():
    """
    测试 NetCDF 文件的加载和写回功能。
    现在通过 df.write(...) 接口完成，不再依赖 NCParser 实例。
    """

    #input_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/test_data.nc"
    #output_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/output_test.nc"

    dataframe_id = "/data/faird/test-data/nc/test_data.nc"
    conn = DacpClient.connect(SERVER_URL, Principal.oauth(TENANT, CLIENT_ID, USERNAME))

    logger.info("🔍 正在加载 DataFrame...")
    df = conn.open(dataframe_id)

    if df is None:
        logger.info("加载失败：faird.open 返回 None。请检查 parser 或文件路径。")
        return

    logger.info("DataFrame 加载成功")
    logger.info(f"Schema: {df.schema}")
    logger.info(f"Columns: {df.column_names}")
    logger.info(f"Number of rows: {df.num_rows}")
    logger.info(f"Memory usage: {df.nbytes} bytes")

    logger.info(f"Filter temperature < 0.08: {df.filter(pc.less(df["temperature"], 0.08))}")

    # 🔍 1. 查看前几行数据（自动触发 data 加载）
    logger.info("\n查看前几行数据预览:")
    logger.info(df.to_string(head_rows=5, tail_rows=0))

    output_path = "/data/faird/test-data/nc/test_data_output.nc"
    logger.info(f"正在使用 df.write(...) 写回文件到: {output_path}")

    try:
        df.write(output_path=output_path)
        logger.info(f"成功写入文件: {output_path}")
    except Exception as e:
        logger.info(f"写入文件失败: {e}")


if __name__ == "__main__":
    test_netcdf_file()