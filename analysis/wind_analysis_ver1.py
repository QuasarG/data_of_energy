import os
import numpy as np
import netCDF4 as nc
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from scipy.spatial import cKDTree

def check_data_completeness(nc_dir):
    """
    检查1990-2023年每月数据是否完整
    返回：
    - 完整性报告DataFrame
    - 按年份分组的文件字典
    """
    expected_years = range(1990, 2024)
    expected_months = range(1, 13)

    # 创建期望的文件名列表
    expected_files = [
        f"{year:04d}-{month:02d}_wind_speed.nc"
        for year in expected_years
        for month in expected_months
    ]

    # 获取实际文件列表
    existing_files = [f for f in os.listdir(nc_dir) if f.endswith('_wind_speed.nc')]

    # 创建完整性检查DataFrame
    completeness_data = []
    files_by_year = {}

    for year in expected_years:
        year_files = [f for f in existing_files if f.startswith(f"{year:04d}")]
        existing_months = [int(f.split('-')[1][:2]) for f in year_files]

        # 记录每月数据状态
        for month in expected_months:
            status = "存在" if month in existing_months else "缺失"
            completeness_data.append({
                "年份": year,
                "月份": month,
                "状态": status
            })

        # 按年份组织文件
        if year_files:
            files_by_year[year] = sorted(year_files)

    completeness_df = pd.DataFrame(completeness_data)

    # 保存完整性报告
    return completeness_df, files_by_year

def load_wind_power_locations(file_path):
    """加载风电场位置数据"""
    if file_path.endswith('.csv'):
        import chardet
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        encoding = result['encoding']
        df = pd.read_csv(file_path, encoding=encoding)
    elif file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format. Only .csv and .xlsx are supported.")

    return df[['Latitude', 'Longitude']]

def find_nearest_grid_points_vectorized(lats, lons, nc_lats, nc_lons):
    """使用KD树批量查找最接近的网格点索引"""
    lon_grid, lat_grid = np.meshgrid(nc_lons, nc_lats)
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))

    tree = cKDTree(grid_points)
    query_points = np.column_stack((lats, lons))

    distances, indices = tree.query(query_points)

    lat_indices = indices // len(nc_lons)
    lon_indices = indices % len(nc_lons)

    return lat_indices, lon_indices

def process_yearly_data(year, nc_files, nc_dir, power_plant_locations):
    """处理某一年的所有数据文件"""
    total_valid_hours = np.zeros(len(power_plant_locations))
    total_hours = 0
    first_file = True
    lat_indices = None
    lon_indices = None

    for nc_file in sorted(nc_files):
        file_path = os.path.join(nc_dir, nc_file)
        print(f'\n处理文件: {nc_file}')

        with nc.Dataset(file_path, 'r') as ds:
            if first_file:
                # 第一个文件时计算网格点索引
                lats = ds.variables['lat'][:]
                lons = ds.variables['lon'][:]
                lat_indices, lon_indices = find_nearest_grid_points_vectorized(
                    power_plant_locations['Latitude'].values,
                    power_plant_locations['Longitude'].values,
                    lats, lons
                )
                first_file = False

            times = ds.variables['time'][:]
            total_hours += len(times)

            # 批量处理时间点
            batch_size = 50
            total_batches = (len(times) + batch_size - 1) // batch_size

            for batch_idx in tqdm(range(total_batches), desc=f'处理{nc_file}'):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(times))

                wind_speeds = ds.variables['wind_speed'][start_idx:end_idx, :, :]

                # 高度修正
                Z1, Z2, Z0 = 10.0, 109.0, 0.03
                wind_speeds = wind_speeds * (np.log(Z2/Z0) / np.log(Z1/Z0))

                for idx in range(len(power_plant_locations)):
                    location_wind_speeds = wind_speeds[:, lat_indices[idx], lon_indices[idx]]
                    if hasattr(location_wind_speeds, 'mask'):
                        location_wind_speeds = location_wind_speeds.filled(-9999)

                    valid_mask = (location_wind_speeds >= 3) & (location_wind_speeds <= 25) & (location_wind_speeds != -9999)
                    total_valid_hours[idx] += float(np.sum(valid_mask))

    # 创建年度结果DataFrame
    results_df = pd.DataFrame({
        'Latitude': power_plant_locations['Latitude'],
        'Longitude': power_plant_locations['Longitude'],
        'valid_hours': total_valid_hours,
        'total_hours': total_hours,
        'year': year,
        'valid_ratio': total_valid_hours / total_hours
    })

    # # 过滤异常数据
    # results_df = results_df[results_df['valid_ratio'] > 0.001]

    return results_df

def main():
    # 设置路径
    nc_dir = r'G:\windspeed'  # 风速数据目录
    output_dir = r'E:\PythonProjiects\Data_of_energy_competition\output'  # 输出目录
    wind_power_csv = r'E:\PythonProjiects\Data_of_energy_competition\filtered_wind_farm.xlsx'

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 检查数据完整性
    print('检查数据完整性...')
    completeness_df, files_by_year = check_data_completeness(nc_dir)

    # 保存完整性报告
    completeness_file = os.path.join(output_dir, 'data_completeness_report.csv')
    completeness_df.to_csv(completeness_file, index=False)
    print(f'数据完整性报告已保存至: {completeness_file}')

    # 加载风机位置数据
    print('加载风机位置数据...')
    power_plants = load_wind_power_locations(wind_power_csv)
    print(f'成功加载 {len(power_plants)} 个风机位置')

    # 处理每年数据
    yearly_stats = []
    for year, files in sorted(files_by_year.items()):
        print(f'\n处理 {year} 年数据...')
        year_results = process_yearly_data(year, files, nc_dir, power_plants)

        # 保存年度详细数据
        detail_file = os.path.join(output_dir, f'wind_turbine_stats_{year}.csv')
        year_results.to_csv(detail_file, index=False)
        print(f'{year}年详细数据已保存至: {detail_file}')

        # 记录年度统计
        mean_ratio = year_results['valid_ratio'].mean()
        yearly_stats.append({
            'year': year,
            'average_valid_ratio': mean_ratio,
            'total_turbines': len(year_results),
            'total_hours': year_results['total_hours'].iloc[0]
        })
        print(f'{year}年平均有效率: {mean_ratio:.2%}')

    # 保存年度统计汇总
    summary_df = pd.DataFrame(yearly_stats)
    summary_file = os.path.join(output_dir, 'yearly_summary.csv')
    summary_df.to_csv(summary_file, index=False)
    print(f'\n年度统计汇总已保存至: {summary_file}')

if __name__ == '__main__':
    main()