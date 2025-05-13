from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame


def test_sdk():
    conn = DacpClient.connect("dacp://localhost:3101", Principal.oauth("conet", "username", "password"))
    #conn = DacpClient.connect("dacp://cern.ac.cn:3101", Principal.anonymous())

    dataset_ids = conn.list_datasets()
    dataframe_ids = conn.list_dataframes(dataset_ids[0])

    #df = conn.open(dataframe_ids[0])
    df = DataFrame(id=dataframe_ids[0])

    #print_str = df.to_string()

    #df.collect()

    for chunk in df.get_stream():
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    print(df.collect().limit(3))

    # compute on server
    print(df.limit(3).select("column1", "column2"))

    # compute locally
    print(df.collect().limit(3).select("column1", "column2"))
    chunks = df.get_stream()

    # compute locally
    print(df.limit(3).collect().select("column1", "column2"))

    for record_batch in df.get_stream(max_chunksize=1024):
        print(record_batch)

def test_sdk_dataframe():
    conn = DacpClient.connect("dacp://localhost:3101", Principal.anonymous())
    dataframe_id = "/Users/yaxuan/Documents/2024/工作/faird/2024-03-快速发版/测试文件/common/2019年中国榆林市沟道信息.csv"
    df = DataFrame(id=dataframe_id)

    print_str = df.to_string()

    df.collect()

    for chunk in df.get_stream():
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")


if __name__ == "__main__":
    test_sdk()
    #test_sdk_dataframe()