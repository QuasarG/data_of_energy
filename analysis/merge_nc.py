import xarray as xr

# 读取两个NetCDF文件
file1 = 'file1.nc'
file2 = 'file2.nc'

# 使用xarray打开两个文件
ds1 = xr.open_dataset(file1)
ds2 = xr.open_dataset(file2)

# 合并两个数据集
# 假设我们要沿着某个维度（例如时间维度）进行合并
# 如果两个文件的时间维度不同，可以使用concat函数
merged_ds = xr.concat([ds1, ds2], dim='time')  # 假设时间维度为'time'

# 如果两个文件的变量或维度完全相同，也可以使用merge函数
# merged_ds = xr.merge([ds1, ds2])

# 将合并后的数据集保存为新的NetCDF文件
output_file = 'merged_output.nc'
merged_ds.to_netcdf(output_file)

print(f"合并完成，结果已保存到 {output_file}")