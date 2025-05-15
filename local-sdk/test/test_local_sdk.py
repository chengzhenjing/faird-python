import faird_local
import pyarrow.compute as pc
import pandas as pd

from dataframe import DataFrame


def test_local_sdk():

    dataframe_id = "/Users/yaxuan/Desktop/测试用/2019年中国榆林市沟道信息.csv"

    """
    0. open dataframe
    """
    df = faird_local.open(dataframe_id)

    """
    1. basic attributes
    """
    schema = df.schema
    column_names = df.column_names
    num_rows = df.num_rows
    num_columns = df.num_columns
    shape = df.shape
    nbytes = df.nbytes

    """
    2. collect data, stream data
    """
    ## 2.1 collect all data
    all_data = df.collect()
    print(f"data size: {all_data.num_rows}")
    ## 2.2 stream data
    stream_data = df.get_stream(max_chunksize=100)
    for chunk in stream_data:
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    """
    3. row & column operations
    """
    ## 3.1 use index and column name to get row and column
    row_0 = df[0]
    column_OBJECTID = df["OBJECTID"]
    cell = df[0]["OBJECTID"]

    ## 3.2 limit, slice, select
    limit_3 = df.limit(3)
    slice_2_5 = df.slice(2, 5)
    select_columns = df.select("OBJECTID", "start_p", "end_p")

    """
    4. filter, map
    """
    ## 4.1 filter
    mask = pc.less(df["OBJECTID"], 30)
    filtered_data = df.filter(mask)

    ## 4.2 map
    mapped_df = df.map("OBJECTID", lambda x: x + 10, new_column_name="OBJECTID_PLUS_10")

    ## 4.3 sum
    sum = df.sum('OBJECTID')

    """
    5. from_pandas(), to_pandas()
    """
    pdf = df.to_pandas()

    pandas_data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
    df_from_pandas = DataFrame.from_pandas(pandas_data)
    print(df_from_pandas)


if __name__ == "__main__":
    test_local_sdk()