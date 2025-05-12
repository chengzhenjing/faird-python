from sdk.dacp_client import DacpClient, Principal


def test_sdk():
    conn = DacpClient.connect("dacp://cern.ac.cn:3101", Principal.oauth("conet", "username", "password"))
    #conn = DacpClient.connect("dacp://cern.ac.cn:3101", Principal.anonymous())

    dataset_ids = conn.list_datasets()
    dataframe_ids = conn.list_dataframes(dataset_ids[0])

    df = conn.open(dataframe_ids[0])

    print("aaa")


if __name__ == "__main__":
    test_sdk()