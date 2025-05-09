from core.models.dataframe import DataFrame
from parser import csv_parser


def open(dataframe_id: str) -> DataFrame:
    """
    打开指定dataframe并返回一个 DataFrame 对象

    Args:
        dataframe_id (str): dataframe 的唯一标识符
    Returns:
        DataFrame: 返回一个 DataFrame 对象
    """

    arrow_table = csv_parser.parse_csv_to_arrow(dataframe_id)
    return DataFrame(id=dataframe_id, data=arrow_table)
