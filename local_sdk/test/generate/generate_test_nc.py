from netCDF4 import Dataset
import numpy as np
import zipfile
import os

# Step 1: 生成 NetCDF 文件
file_path = "test_data.nc"

with Dataset(file_path, 'w', format='NETCDF4') as ds:
    # 创建维度
    time_dim = ds.createDimension('time', 1093)
    lat_dim = ds.createDimension('lat', 180)
    lon_dim = ds.createDimension('lon', 360)

    # 创建变量
    times = ds.createVariable('time', 'f8', ('time',))
    lats = ds.createVariable('lat', 'f4', ('lat',))
    lons = ds.createVariable('lon', 'f4', ('lon',))
    temp = ds.createVariable('temperature', 'f4', ('time', 'lat', 'lon'))

    # 填充数据
    times[:] = np.arange(1093)
    lats[:] = np.linspace(-90, 90, 180)
    lons[:] = np.linspace(-180, 180, 360)

    # 创建三维温度数据（时间、纬度、经度）
    temp_data = np.random.rand(1093, 180, 360).astype(np.float32)
    temp[:, :, :] = temp_data

print(f"✅ 已生成测试 NetCDF 文件：{file_path}")

# Step 2: 打包成 ZIP
zip_filename = "test_nc_data.zip"
with zipfile.ZipFile(zip_filename, 'w') as zipf:
    zipf.write(file_path)

print(f"📦 已打包为 {zip_filename}")
