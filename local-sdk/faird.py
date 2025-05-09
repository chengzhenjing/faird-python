from core.models.dataframe import DataFrame
import pyarrow as pa

def open(file_path: str) -> DataFrame:
    """
    打开指定文件并返回一个 DataFrame 对象
    :param file_path: 文件路径
    :return: DataFrame 对象
    """
    # 模拟从文件加载数据
    # 这里可以根据实际需求替换为从文件或其他数据源加载 pyarrow.Table
    data = pa.Table.from_pydict({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })
    return DataFrame(id="example", data=data)