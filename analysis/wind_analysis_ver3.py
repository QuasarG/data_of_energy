import os
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from tqdm import tqdm
import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import partial

warnings.filterwarnings('ignore')

# ----------------------
# 配置参数
# ----------------------
wind_dir = Path(r"G:/windspeed")
roughness_dir = Path(r"G:/monthly aerodynamic roughness length dataset")
output_dir = Path(r"E:\PythonProjiects\Data_of_energy_competition\output")
farm_file = Path(r"E:\PythonProjiects\Data_of_energy_competition\preprocessing\filtered_wind_farm.xlsx")


# ----------------------
# 全局函数定义 (确保可序列化)
# ----------------------
def find_nearest(lat_array, lon_array, target_lat, target_lon):
    """查找最近网格索引"""
    lat_idx = np.abs(lat_array - target_lat).argmin()
    lon_idx = np.abs(lon_array - (target_lon % 360)).argmin()  # 处理经度环绕
    return lat_idx, lon_idx


def process_row(row, wind_coords, z0_coords):
    """处理单行数据"""
    try:
        # 解包坐标数据
        wind_lat, wind_lon = wind_coords['lat'], wind_coords['lon']
        z0_lat, z0_lon = z0_coords['lat'], z0_coords['lon']

        # 计算索引
        w_lat_idx, w_lon_idx = find_nearest(wind_lat, wind_lon, row.Latitude, row.Longitude)
        z_lat_idx, z_lon_idx = find_nearest(z0_lat, z0_lon, row.Latitude, row.Longitude)
        return (w_lat_idx, w_lon_idx, z_lat_idx, z_lon_idx)
    except Exception as e:
        print(f"处理坐标({row.Latitude}, {row.Longitude})时出错: {str(e)}")
        return (-1, -1, -1, -1)


# ----------------------
# 预处理阶段：生成月平均粗糙度
# ----------------------
def generate_monthly_z0mean():
    """生成2010-2020年各月平均粗糙度文件"""
    avg_z0_dir = output_dir / "monthly_average_z0"
    avg_z0_dir.mkdir(parents=True, exist_ok=True)

    for month in tqdm(range(1, 13), desc="生成月平均粗糙度"):
        # 收集同月所有年份文件
        pattern = f"*{month:02d}15_global_monthly_z0m_25km.nc"
        files = list(roughness_dir.glob(pattern))

        if not files:
            print(f"警告: 未找到{month:02d}月的粗糙度文件")
            continue

        # 计算时空平均
        with xr.open_mfdataset(files, combine='nested', concat_dim='time') as ds:
            z0_mean = ds['Monthly_z0m_25km'].mean(dim='time', skipna=True)
            z0_mean = z0_mean.where(z0_mean != 0)  # 处理缺失值标记

        # 保存为相同格式的新文件
        output_file = avg_z0_dir / f"mean_{month:02d}_z0m.nc"
        z0_mean.to_netcdf(
            output_file,
            encoding={'Monthly_z0m_25km': {'zlib': True, 'complevel': 5}}
        )


# ----------------------
# 预处理阶段：建立风电场索引
# ----------------------
def precompute_farm_indices():
    """预计算风电场索引（Windows兼容版）"""
    # 加载样本数据
    with xr.open_dataset(next(wind_dir.glob("*.nc"))) as sample_wind:
        wind_coords = {
            'lat': sample_wind.lat.values,
            'lon': sample_wind.lon.values
        }

    with xr.open_dataset(next(roughness_dir.glob("*.nc"))) as sample_z0:
        z0_coords = {
            'lat': sample_z0.lat.values,
            'lon': sample_z0.lon.values
        }

    # 加载风电场位置
    farms = pd.read_excel(farm_file)
    print(f"正在处理 {len(farms)} 个风电场...")

    # 准备参数列表
    params = [(row, wind_coords, z0_coords) for row in farms.itertuples()]

    # 多进程处理
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(
            executor.map(partial(process_row, wind_coords=wind_coords, z0_coords=z0_coords),
                         farms.itertuples()),
            total=len(farms),
            desc="预计算索引"
        ))

    # 保存结果
    farms[['wind_lat', 'wind_lon', 'z0_lat', 'z0_lon']] = results
    valid_farms = farms[(farms['wind_lat'] >= 0) & (farms['z0_lat'] >= 0)]
    valid_farms.to_csv(output_dir / "wind_farm_indices.csv", index=False)
    print(f"成功处理 {len(valid_farms)}/{len(farms)} 个有效风电场")


# ----------------------
# 核心处理函数
# ----------------------
def process_windspeed():
    # 加载预计算数据
    try:
        farms = pd.read_csv(output_dir / "wind_farm_indices.csv")
    except FileNotFoundError:
        print("错误: 未找到预计算的索引文件，请先运行预处理步骤")
        return

    all_results = []

    # 处理每个风速文件
    wind_files = list(wind_dir.glob("*.nc"))
    for wind_file in tqdm(wind_files, desc="处理风速文件"):
        try:
            # 解析时间信息
            year, month = map(int, wind_file.stem.split("_")[0].split("-"))

            # 加载粗糙度数据
            if 2010 <= year <= 2020:
                z0_file = roughness_dir / f"{year}{month:02d}15_global_monthly_z0m_25km.nc"
            else:
                z0_file = output_dir / "monthly_average_z0" / f"mean_{month:02d}_z0m.nc"

            if not z0_file.exists():
                print(f"警告: 未找到粗糙度文件 {z0_file}")
                continue

            with xr.open_dataset(z0_file) as z0_data:
                z0_values = z0_data['Monthly_z0m_25km'].where(z0_data['Monthly_z0m_25km'] > 0)

                # 加载风速数据
                with xr.open_dataset(wind_file) as wind_data:
                    u10 = wind_data['wind_speed'].where(wind_data['wind_speed'] != -9999.0)

                    # 处理每个风电场
                    for _, farm in farms.iterrows():
                        try:
                            # 提取粗糙度
                            z0 = z0_values.isel(
                                lat=int(farm['z0_lat']),
                                lon=int(farm['z0_lon'])
                            ).item()

                            if np.isnan(z0) or z0 <= 0:
                                continue

                            # 提取风速
                            u10_series = u10.isel(
                                lat=int(farm['wind_lat']),
                                lon=int(farm['wind_lon'])
                            ).values

                            # 计算调整系数
                            with np.errstate(divide='ignore'):
                                adjustment = np.log(109 / z0) / np.log(10 / z0)

                            # 调整风速并统计有效时间
                            u109 = u10_series * adjustment
                            valid_hours = np.sum((u109 >= 5) & (u109 <= 20) & (~np.isnan(u109)))

                            all_results.append({
                                'year': year,
                                'month': month,
                                'lat': farm['Latitude'],
                                'lon': farm['Longitude'],
                                'valid_hours': valid_hours
                            })

                        except Exception as e:
                            print(f"处理风电场({farm['Latitude']}, {farm['Longitude']})时出错: {str(e)}")

        except Exception as e:
            print(f"处理文件 {wind_file} 时发生严重错误: {str(e)}")
            continue

    # 汇总结果
    result_df = pd.DataFrame(all_results)
    if not result_df.empty:
        annual_stats = result_df.groupby(['year', 'lat', 'lon'])['valid_hours'].sum().reset_index()
        annual_stats.to_csv(output_dir / "annual_valid_hours.csv", index=False)
        print("处理完成，结果已保存")
    else:
        print("警告: 未生成任何有效结果")


# ----------------------
# 主执行流程
# ----------------------
if __name__ == "__main__":
    # 创建输出目录
    output_dir.mkdir(parents=True, exist_ok=True)

    # 步骤1：生成月平均粗糙度文件
    if not (output_dir / "monthly_average_z0").exists():
        generate_monthly_z0mean()

    # 步骤2：预计算风电场索引
    if not (output_dir / "wind_farm_indices.csv").exists():
        precompute_farm_indices()

    # 步骤3：处理所有风速数据
    process_windspeed()