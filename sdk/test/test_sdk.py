from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame


def test_sdk():
    conn = DacpClient.connect("dacp://localhost:3101", Principal.oauth("conet", "faird-user1", "user1@cnic.cn"))
    #conn = DacpClient.connect("dacp://cern.ac.cn:3101", Principal.anonymous())

    datasets = conn.list_datasets()
    dataframe_ids = conn.list_dataframes(datasets[0].get('name'))

    #df = conn.open(dataframe_ids[0])
    df = DataFrame(id=dataframe_ids[0])
    schema = df.schema
    num_rows = df.num_rows

    #print / to_string
    #1. remote
    # print(df)
    # df1 = df.limit(3)
    # print(df1)
    # print(df1)
    # df2 = df1.select("OBJECTID")
    # print(df2)
    # print(df)
    # print(df.limit(3).select("end_p"))
    #
    # # 2. local
    # df3 = df.collect().limit(3) # 预期改变df.data，而不是生成新的df，之后对df的操作都在内存中进行
    # print(df3)
    # df4 = df3.select("OBJECTID")
    # print(df4)
    # print(df)
    # print(df.select("end_p"))
    #
    # # remote + local
    # df5 = df.limit(3).collect() # 在server计算后，只返回部分数据，df5.data只包含limit后的结果数据；df.data仍然是None
    # print(df5)
    # print(df)
    # df6 = df.limit(3).collect().select("OBJECTID")
    # print(df6)
    # print(df)
    #
    #
    # # streaming
    for chunk in df.get_stream(): # 默认1000行
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    print(df.collect().limit(4))

    # compute on server
    print(df.limit(3).select("column1", "column2"))

    # compute locally
    print(df.collect().limit(3).select("column1", "column2"))
    chunks = df.get_stream()

    # compute locally
    print(df.limit(3).collect().select("column1", "column2"))


if __name__ == "__main__":
    test_sdk()