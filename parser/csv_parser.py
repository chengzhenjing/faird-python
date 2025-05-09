import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import os

DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/csv/")

def parse_csv_to_arrow(dataframe_id) -> pa.Table:
    """
    将 CSV 文件保存为 .arrow 文件，并以零拷贝方式加载为内存中的 pyarrow Table

    Args:
        dataframe_id (str): 输入 CSV 文件路径
    Returns:
        pa.Table: 返回一个 pyarrow 表对象
    """

    # 确保缓存目录存在
    os.makedirs(os.path.dirname(DEFAULT_ARROW_CACHE_PATH), exist_ok=True)

    # 提取文件名并拼接 .arrow 后缀
    arrow_file_name = os.path.basename(dataframe_id).rsplit(".", 1)[0] + ".arrow"
    arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

    # 读取 CSV 文件为 pyarrow Table
    table = csv.read_csv(dataframe_id)

    # 保存为 .arrow 文件
    with ipc.new_file(arrow_file_path, table.schema) as writer:
        writer.write_table(table)
    print(f"成功将 {dataframe_id} 保存为 {arrow_file_path}")

    # 使用 pa.memory_map 零拷贝方式加载 .arrow 文件为 pyarrow Table
    with pa.memory_map(arrow_file_path, "r") as source:
        return ipc.open_file(source).read_all()