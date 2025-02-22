'''数据规划: 
    所有风机地理位置(GIS)；已有经纬度数据；
    1990年到2024年全球风速数据（nc）格式；
    风速数据格式例子：（所有nc文件在一个目录中）
    netcdf G:\windspeed\1990-04_wind_speed.nc {
  dimensions:
    time = UNLIMITED;   // (719 currently)
    lat = 1801;
    lon = 3600;
  variables:
    int time(time=719);
      :units = "YYYYMMDD";
      :_ChunkSizes = 1024U; // uint

    float lat(lat=1801);
      :units = "degrees_north";

    float lon(lon=3600);
      :units = "degrees_east";

    float wind_speed(time=719, lat=1801, lon=3600);
      :_FillValue = -9999.0f; // float
      :units = "m/s";
      :long_name = "10m wind speed";
      :_ChunkSizes = 1U, 901U, 1800U; // uint
    }
小于5m/s和大于25m/s风机暂停不转
目标：
    筛选所有时段内:风速小于 5m/s 和大于 25m/s 的时间段.
    计算风机当年度无法正常运作的时间与全年时间的比值
'''

import os
import numpy as np
import netCDF4 as nc
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from scipy.spatial import cKDTree

def load_wind_power_locations(csv_path):
    """
    加载风电场位置数据
    返回包含经纬度信息的DataFrame
    """
    df = pd.read_csv(csv_path)
    return df[['latitude', 'longitude']]

def find_nearest_grid_points_vectorized(lats, lons, nc_lats, nc_lons):
    """
    使用KD树批量查找最接近的网格点索引，使用meshgrid优化网格点生成
    """
    # 创建网格点坐标
    lon_grid, lat_grid = np.meshgrid(nc_lons, nc_lats)
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
    
    # 构建KD树
    tree = cKDTree(grid_points)
    
    # 准备查询点
    query_points = np.column_stack((lats, lons))
    
    # 批量查找最近点
    distances, indices = tree.query(query_points)
    
    # 转换为二维索引
    lat_indices = indices // len(nc_lons)
    lon_indices = indices % len(nc_lons)
    
    return lat_indices, lon_indices

def analyze_wind_speed(nc_file_path, power_plant_locations):
    """分析风速数据，计算每个风机无法正常运作的时间比例"""
    try:
        print(f'\n开始处理文件: {os.path.basename(nc_file_path)}')
        year = int(os.path.basename(nc_file_path)[:4])
        
        with nc.Dataset(nc_file_path, 'r') as ds:
            lats = ds.variables['lat'][:]
            lons = ds.variables['lon'][:]
            times = ds.variables['time'][:]
            total_hours = len(times)
            print(f'文件包含 {total_hours} 个时间点')
            
            # 一次性计算所有风机位置对应的网格点索引
            lat_indices, lon_indices = find_nearest_grid_points_vectorized(
                power_plant_locations['latitude'].values,
                power_plant_locations['longitude'].values,
                lats, lons
            )
            
            # 创建结果DataFrame
            results_df = power_plant_locations.copy()
            results_df['invalid_hours'] = 0
            results_df['total_hours'] = total_hours
            results_df['invalid_ratio'] = 0.0
            results_df['year'] = year
            
            # 设置批处理大小
            batch_size = 50  # 每批处理50个时间点
            total_batches = (len(times) + batch_size - 1) // batch_size
            
            for batch_idx in tqdm(range(total_batches), desc='处理时间批次'):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(times))
                
                # 批量读取部分时间点的风速数据
                wind_speeds = ds.variables['wind_speed'][start_idx:end_idx, :, :]
                
                # 将10m高度风速转换为109m高度风速
                Z1 = 10.0  # 原始高度
                Z2 = 109.0  # 目标高度
                Z0 = 0.06  # 粗糙度参数
                wind_speeds = wind_speeds * (np.log(Z2/Z0) / np.log(Z1/Z0))
                
                # 计算每个风机位置的无效时间
                for idx in tqdm(range(len(power_plant_locations)), desc='处理风机位置', leave=False):
                    location_wind_speeds = wind_speeds[:, lat_indices[idx], lon_indices[idx]]
                    # 处理掩码数据，将掩码值转换为False
                    if hasattr(location_wind_speeds, 'mask'):
                        location_wind_speeds = location_wind_speeds.filled(-9999)
                    invalid_mask = (location_wind_speeds < 5) | (location_wind_speeds > 25) | (location_wind_speeds == -9999)
                    invalid_count = np.sum(invalid_mask)
                    results_df.loc[idx, 'invalid_hours'] += float(invalid_count)
                    
                    if idx % 100 == 0:
                        current_ratio = results_df.loc[idx, 'invalid_hours'] / total_hours
                        print(f'\r当前处理进度: {idx+1}/{len(power_plant_locations)} 风机, ' \
                              f'当前风机无效率: {current_ratio:.2%}', end='')
                
                # 清理内存
                del wind_speeds
            
            # 计算每个风机的无效率
            results_df['invalid_ratio'] = results_df['invalid_hours'] / results_df['total_hours']
            
            # 保存结果到CSV文件
            output_file = f'wind_turbine_stats_{year}.csv'
            results_df.to_csv(output_file, index=False)
            print(f'\n\n结果已保存到: {output_file}')
            
            # 显示前10个风机的结果
            print('\n前10个风机的统计结果:')
            pd.set_option('display.float_format', lambda x: '{:.2f}'.format(x))
            print(results_df[['latitude', 'longitude', 'invalid_ratio']].head(10))
            
            # 计算总体无效率
            total_ratio = results_df['invalid_ratio'].mean()
            print(f'\n{year}年 - 所有风机平均无效率: {total_ratio:.2%}')
            return {year: total_ratio}
    
    except Exception as e:
        print(f'\n处理文件时出错: {str(e)}')
        return None
    
    # 计算并输出结果
    if yearly_stats[year]['total_hours'] > 0:
        ratio = yearly_stats[year]['invalid_hours'] / \
               (yearly_stats[year]['total_hours'] * len(power_plant_locations))
        print(f'\n文件处理完成，无效率: {ratio:.2%}')
        return {year: ratio}
    
    return None

def main():
    nc_file = r'G:\windspeed\2023-12_wind_speed.nc'  # 指定单个文件进行测试
    wind_power_csv = r'E:\大二上\竞赛\能经大赛\data\globalpowerplantdatabasev130\pure_wind_power_plant_database.csv'
    
    print('开始加载风机位置数据...')
    power_plants = load_wind_power_locations(wind_power_csv)
    print(f'成功加载 {len(power_plants)} 个风机位置')
    
    print('\n开始分析风速数据...')
    result = analyze_wind_speed(nc_file, power_plants)
    if result is not None:
        for year, ratio in result.items():
            print(f'\n总体统计结果 - {year}年风机无法正常运作时间比例: {ratio:.2%}')

if __name__ == '__main__':
    main()