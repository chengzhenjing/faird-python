from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame


def test_sdk():

    conn = DacpClient.connect("dacp://localhost:3101", Principal.oauth("conet", "faird-user1", "user1@cnic.cn"))
    #conn = DacpClient.connect("dacp://10.0.89.38:3101", Principal.oauth("conet", "faird-user1", "user1@cnic.cn"))
    #conn = DacpClient.connect("dacp://47.111.98.226:3101", Principal.oauth("conet", "faird-user1", "user1@cnic.cn"))

    #datasets = conn.list_datasets()
    #datasets = conn.list_datasets(page=1, limit=1000)
    #dataframe_ids = conn.list_dataframes(datasets[0].get('name'))

    #df = conn.open(dataframe_ids[1])

    df = conn.open("/Users/yaxuan/Desktop/测试用/2019年中国榆林市沟道信息.csv")

    df.collect()
    # 1. filter
    print(df.filter("OBJECTID <= 30"))

    # 2. sum
    print(df.sum("OBJECTID"))

    # mean
    print(df.mean("OBJECTID"))
    print(df.min("start_l"))
    print(df.max("start_l"))

    # 3. sort
    print(df.limit(3).sort('OBJECTID', order='descending'))

    # 4. sql
    print(df.sql("select OBJECTID, start_l, end_l "
                 "from dataframe "
                 "where OBJECTID <= 30 "
                 "order by OBJECTID desc "))





    """
    1. compute remotely
    """
    print(df.schema)
    print(df.num_rows)
    print(df)
    print(df.limit(5).select("OBJECTID", "start_p", "end_p"))
    #print(df.limit(5).select("lat", "lon", "temperature"))

    """
    2. compute locally
    """
    print(df.collect().limit(3).select("from_node"))
    #print(df.collect().limit(3).select("temperature"))

    """
    2. compute remote & local
    """
    print(df.limit(3).collect().select("OBJECTID", "start_p", "end_p"))
    #print(df.limit(3).collect().select("lat", "lon", "temperature"))

    # streaming
    for chunk in df.get_stream(): # 默认1000行
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")


if __name__ == "__main__":
    test_sdk()