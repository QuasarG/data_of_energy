import os
import numpy as np
import netCDF4 as nc
import pandas as pd
from tqdm import tqdm
from scipy.spatial import cKDTree
import logging

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='processing.log'
)

# 常量定义
WIND_DIR = r"E:\PythonProjiects\Data_of_energy_competition\filtered_wind_farm.xlsx"
Z0_DIR = "path/to/z0_files"
CSV_FILE = "path/to/power_plants.csv"
TARGET_HEIGHT = 109  # 目标高度（米）
REFERENCE_HEIGHT = 10  # 参考高度（米）
VALID_RANGE = (5, 20)  # 有效风速范围（m/s）
YEARS = range(2010, 2021)  # 需要处理的年份范围

def load_power_plants(csv_path):
    """加载风力发电场数据"""
    try:
        power_plants = pd.read_csv(csv_path)
        # 转换经度到 0-360 范围
        power_plants['Longitude'] = power_plants['Longitude'].apply(lambda x: x + 360 if x < 0 else x)
        return power_plants
    except Exception as e:
        raise ValueError(f"加载发电场数据失败: {e}")

def preprocess_z0_indices(z0_dir, power_plants):
    """预处理粗糙度索引"""
    z0_files = sorted([f for f in os.listdir(z0_dir) if f.endswith('.nc')])
    lats_z0, lons_z0 = None, None

    # 获取基准网格
    with nc.Dataset(os.path.join(z0_dir, z0_files[0])) as ds:
        lats_z0 = ds['lat'][:]
        lons_z0 = ds['lon'][:]
        lons_z0 = np.where(lons_z0 < 0, lons_z0 + 360, lons_z0)

    # 计算最近网格点索引
    lat_idx, lon_idx = find_nearest_grid_points(
        power_plants['Latitude'].values,
        power_plants['Longitude'].values,
        lats_z0, lons_z0
    )
    power_plants['z0_lat_idx'] = lat_idx
    power_plants['z0_lon_idx'] = lon_idx

    return power_plants

def find_nearest_grid_points(lats, lons, nc_lats, nc_lons):
    """找到最近的网格点"""
    lon_grid, lat_grid = np.meshgrid(nc_lons, nc_lats)
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
    tree = cKDTree(grid_points)
    query_points = np.column_stack((lats, lons))
    _, indices = tree.query(query_points)
    lat_idx = indices // len(nc_lons)
    lon_idx = indices % len(nc_lons)
    return lat_idx, lon_idx

def calculate_average_z0(z0_dir, z0_files, power_plants):
    """计算每个地点的多年平均粗糙度"""
    avg_z0 = np.zeros(len(power_plants))
    z0_values = []

    for file in tqdm(z0_files, desc="计算多年平均粗糙度"):
        with nc.Dataset(os.path.join(z0_dir, file)) as ds:
            z0_data = ds['Monthly_z0m_25km'][:]
            for i, row in power_plants.iterrows():
                lat_idx = row['z0_lat_idx']
                lon_idx = row['z0_lon_idx']
                value = z0_data[lat_idx, lon_idx]
                if value > 0:  # 排除无效值
                    z0_values.append(value)

    global_avg = np.mean(z0_values) if z0_values else 0.03
    avg_z0[:] = global_avg
    power_plants['avg_z0'] = avg_z0
    return power_plants

def process_monthly_file(wind_path, z0_dir, power_plants):
    """处理单个月份文件"""
    valid_hours = np.zeros(len(power_plants), dtype=np.int32)
    total_time = 0

    # 获取对应粗糙度文件
    year_month = os.path.basename(wind_path).split('_')[0]
    z0_file = f"{year_month[:4]}{year_month[5:7]}15_global_monthly_z0m_25km.nc"
    z0_path = os.path.join(z0_dir, z0_file)

    # 加载粗糙度数据
    if os.path.exists(z0_path):
        with nc.Dataset(z0_path) as ds:
            z0_data = ds['Monthly_z0m_25km'][:]
            current_z0 = z0_data[
                power_plants['z0_lat_idx'].values,
                power_plants['z0_lon_idx'].values
            ]
            valid_mask = (current_z0 > 0) & ~np.isnan(current_z0)
            current_z0[~valid_mask] = power_plants['avg_z0'][~valid_mask].values
    else:
        current_z0 = power_plants['avg_z0'].values

    # 计算修正系数
    Z1, Z2 = REFERENCE_HEIGHT, TARGET_HEIGHT
    with np.errstate(divide='ignore', invalid='ignore'):
        coeff = np.log(Z2 / current_z0) / np.log(Z1 / current_z0)
        coeff = np.nan_to_num(coeff, nan=1.0)

    # 处理风速数据
    with nc.Dataset(wind_path) as ds:
        times = ds.dimensions['time'].size
        lats = ds['lat'][:]
        lons = ds['lon'][:]
        lons = np.where(lons < 0, lons + 360, lons)

        # 预计算坐标索引
        lat_idx, lon_idx = find_nearest_grid_points(
            power_plants['Latitude'].values,
            power_plants['Longitude'].values,
            lats, lons
        )

        for start in range(0, times, 50):  # 分块处理
            end = min(start + 50, times)
            wind_slice = ds['wind_speed'][start:end, :, :]

            # 提取对应位置风速
            wind_speeds = wind_slice[:, lat_idx, lon_idx].T
            adjusted = wind_speeds * coeff[:, np.newaxis]

            # 统计有效时间
            valid = np.logical_and(adjusted >= VALID_RANGE[0], adjusted <= VALID_RANGE[1])
            valid_hours += np.sum(valid, axis=1)
            total_time += (end - start)

    return valid_hours, total_time

def main():
    # 加载发电场数据
    power_plants = load_power_plants(CSV_FILE)

    # 预处理粗糙度索引
    power_plants = preprocess_z0_indices(Z0_DIR, power_plants)

    # 计算多年平均粗糙度
    z0_files = sorted([f for f in os.listdir(Z0_DIR) if f.endswith('.nc')])
    power_plants = calculate_average_z0(Z0_DIR, z0_files, power_plants)

    # 处理每年数据
    for year in YEARS:
        logging.info(f"开始处理年份：{year}")
        yearly_valid = np.zeros(len(power_plants), dtype=np.int32)
        total_time = 0

        # 获取当年所有风速文件
        wind_files = [f for f in os.listdir(WIND_DIR)
                     if f.startswith(f"{year}-") and f.endswith('.nc')]

        for wind_file in tqdm(wind_files, desc=f'处理{year}年'):
            wind_path = os.path.join(WIND_DIR, wind_file)
            valid_hours, time_count = process_monthly_file(
                wind_path,
                Z0_DIR,
                power_plants
            )
            yearly_valid += valid_hours
            total_time += time_count

        # 计算可用时间百分比
        availability = (yearly_valid / total_time) * 100 if total_time > 0 else 0

        # 保存结果
        result_df = pd.DataFrame({
            'Latitude': power_plants['Latitude'],
            'Longitude': power_plants['Longitude'],
            'valid_hours': yearly_valid,
            'availability': availability
        })
        result_df.to_csv(f'wind_availability_{year}.csv', index=False)
        logging.info(f"完成{year}年处理，结果已保存")

if __name__ == "__main__":
    main()