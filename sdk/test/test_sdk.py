import json

from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame
import os
import uuid


def test_sdk():

    #url = "dacp://localhost:3101"
    #url = "dacp://10.0.89.38:3101"
    url = "dacp://60.245.194.25:50201"
    username = "faird-user1"
    password = "user1@cnic.cn"

    conn = DacpClient.connect(url, Principal.oauth("conet", username=username, password=password))
    #conn = DacpClient.connect(url, Principal.oauth("controld", controld_domain_name="controld_domain_name", signature="signature"))
    #conn = DacpClient.connect(url)
    #conn = DacpClient.connect(url, Principal.ANONYMOUS)

    datasets = conn.list_datasets()
    metadata = conn.get_dataset(datasets[66])
    dataframes = conn.list_dataframes(datasets[66])
    dataframe_name = dataframes[6]['dataframeName']
    #dataframe_name = 'dacp://60.245.194.25:50201/GFS全球预报系统数据（实时更新）/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/Instructions_for_Read_SOCATv3_v2021.pdf/2019年中国榆林市沟道信息.csv'
    df = conn.open(dataframe_name)
    #df = conn.open("/Users/yaxuan/Desktop/测试用/2019年中国榆林市沟道信息.csv")

    logger.info(f"表结构: {df.schema} \n")
    logger.info(f"表大小: {df.shape} \n")
    logger.info(f"行数: {df.num_rows} \n")  # 或者len(dataframe)
    logger.info(f"列数: {df.num_cols} \n")
    logger.info(f"列名: {df.column_names} \n")
    logger.info(f"数据大小: {df.total_bytes} \n")

    data_str = df.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False)
    logger.info(f"打印dataframe：\n {data_str}\n")  # 或者直接用logger.info(df)

    # 默认1000行
    for chunk in df.get_stream():
        logger.info(chunk)
        logger.info(f"Chunk size: {chunk.num_rows}")

    # 设置每次读取100行
    for chunk in df.get_stream(max_chunksize=100):
        logger.info(chunk)
        logger.info(f"Chunk size: {chunk.num_rows}")
    #
    # logger.info(f"=== 01.limit, select 在本地计算: === \n {df.collect().limit(3).select("start_p")} \n")
    # logger.info(f"=== 02.limit, select 在远程计算，仅将处理结果加载到本地: === \n {df.limit(3).select("start_p").collect()} \n")
    # logger.info(f"=== 03.limit 在远程计算，select 在本地计算: === \n {df.limit(3).collect().select("start_p")} \n")

    logger.info(f"打印指定列的值: \n {df["start_p"]} \n")
    logger.info(f"筛选某几列: \n {df.select("start_p", "start_l", "end_l")} \n")

    logger.info(f"打印第0行的值: \n {df[0]} \n")
    logger.info(f"打印第0行、指定列的值: \n {df[0]["start_l"]} \n")
    logger.info(f"筛选前10行: \n {df.limit(10)} \n")
    logger.info(f"筛选第2-4行: \n {df.slice(2, 4)} \n")

    # 示例 1: 筛选某列值大于 10 的行
    expression = "OBJECTID <= 30"

    # 示例 2: 筛选某列值等于特定字符串的行
    expression = "name == 'example'"

    # 示例 3: 筛选多列满足条件的行
    expression = "(OBJECTID > 10) & (OBJECTID < 50)"

    # 示例 4: 筛选某列值在特定列表中的行
    expression = "OBJECTID.isin([11, 12, 13])"

    # 示例 5: 筛选某列值不为空的行
    expression = "name.notnull()"

    # 示例 6: 复杂条件组合
    expression = "((OBJECTID < 10) | (name == 'example')) & (start_p != 0)"

    logger.info(f"条件筛选后的结果: \n {df.filter(expression)} \n")

    ## 8.1 sum
    logger.info(f"统计某一数值列的和: {df.sum("start_l")}")
    ## 8.2 mean
    logger.info(f"统计某一数值列的平均值: {df.mean("start_l")}")
    ## 8.3 min
    logger.info(f"统计某一数值列的最小值: {df.min("start_l")}")
    ## 8.4 max
    logger.info(f"统计某一数值列的最大值: {df.max("start_l")}")

    logger.info(f"按照某个列的值升序或者降序: \n {df.sort('start_l', order='descending')}")

    ## 10.1 sql
    sql_str = ("select OBJECTID, start_l, end_l "
               "from dataframe "
               "where OBJECTID <= 30 "
               "order by OBJECTID desc ")

    logger.info(f"sql执行结果: {df.sql(sql_str)}")



if __name__ == "__main__":
    file_path = "/"
    arrow_file_name = str(uuid.uuid4()) + ".arrow"
    test_sdk()