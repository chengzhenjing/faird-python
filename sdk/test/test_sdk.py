from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame


def test_sdk():
    conn = DacpClient.connect("dacp://localhost:3101", Principal.oauth("conet", "faird-user1", "user1@cnic.cn"))

    datasets = conn.list_datasets()
    dataframe_ids = conn.list_dataframes(datasets[0].get('name'))

    dataframe_ids = [
        "/Users/yaxuan/Desktop/测试用/2019年中国榆林市沟道信息.csv",
        "/Users/yaxuan/Desktop/测试用/sample.tiff",
        "/Users/yaxuan/Desktop/测试用/test_data.nc"
    ]

    df = conn.open(dataframe_ids[0])

    """
    1. compute remotely
    """
    print(df.schema)
    print(df.num_rows)
    print(df)
    #print(df.limit(5).select("OBJECTID", "start_p", "end_p"))
    print(df.limit(5).select("lat", "lon", "temperature"))

    """
    2. compute locally
    """
    #print(df.collect().limit(3).select("from_node"))
    print(df.collect().limit(3).select("temperature"))

    """
    2. compute remote & local
    """
    #print(df.limit(3).collect().select("OBJECTID", "start_p", "end_p"))
    print(df.limit(3).collect().select("lat", "lon", "temperature"))

    # streaming
    for chunk in df.get_stream(): # 默认1000行
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")


if __name__ == "__main__":
    test_sdk()