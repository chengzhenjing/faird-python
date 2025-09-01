from io import BytesIO

from dfwriter.dfwriter import DfWriter
from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame

if __name__ == "__main__":
    url = "dacp://localhost:3101"
    conn = DacpClient.connect(url)
    dataframe_name = "dacp://0.0.0.0:3101/SOCATv2021/SOCATv2021/SOCATv2021_tracks_gridded_decadal.csv"
    df = conn.open(dataframe_name)

    # 输出到 CSV 文件
    dfwriter = DfWriter()
    dfwriter.output("/Users/zjcheng/Desktop/output/output.csv").format("csv").write(df)

    dfwriter.output("/Users/zjcheng/Desktop/output/output.arrow").write(df)  # 默认是ARROW格式

    # 输出到字节流
    buffer = BytesIO()
    dfwriter.output(buffer).format("csv").write(df)
    print("written to buffer:", buffer.getvalue())

    dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/sample.tiff"
    df = conn.open(dataframe_name)
    dfwriter = DfWriter()
    dfwriter.output("/Users/yaxuan/Desktop/output/output.tiff").format("tiff").write(df)

    #dfwriter.output("/Users/yaxuan/Desktop/output/output.nc").format("nc").write(df)

