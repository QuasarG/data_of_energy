import os
import numpy as np
import netCDF4 as nc
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from scipy.spatial import cKDTree


def check_data_completeness(nc_dir):
    """检查数据完整性并按年份组织文件"""
    files = [f for f in os.listdir(nc_dir) if f.endswith('.nc')]
    if not files:
        raise ValueError("未找到数据文件")

    # 按年份组织文件
    files_by_year = {}
    for file in files:
        try:
            year = int(file.split('-')[0])
            if 2010 <= year <= 2020:
                if year not in files_by_year:
                    files_by_year[year] = []
                files_by_year[year].append(file)
        except Exception as e:
            print(f"解析文件{file}时出错: {str(e)}")

    # 检查每年的文件数量
    completeness = []
    for year in range(2010, 2021):
        n_files = len(files_by_year.get(year, []))
        completeness.append({
            'year': year,
            'files_count': n_files,
            'is_complete': n_files == 12
        })

    return pd.DataFrame(completeness), files_by_year


def load_wind_power_locations(file_path):
    """加载风电场位置数据"""
    try:
        df = pd.read_excel(file_path)
        required_cols = ['Latitude', 'Longitude']
        if not all(col in df.columns for col in required_cols):
            raise ValueError("输入文件缺少必要的列")
        return df
    except Exception as e:
        raise Exception(f"加载风电场位置数据失败: {str(e)}")


def preprocess_roughness_data(z0_dir, power_plants):
    """预处理粗糙度数据并计算平均Z0"""
    print("开始处理粗糙度数据...")

    z0_files = [f for f in os.listdir(z0_dir) if f.endswith('.nc')]
    if not z0_files:
        raise ValueError("未找到粗糙度数据文件")

    # 加载样本文件获取经纬度信息
    sample_file = os.path.join(z0_dir, z0_files[0])
    with nc.Dataset(sample_file, 'r') as ds:
        lons_z0 = ds.variables['lon'][:]
        lats_z0 = ds.variables['lat'][:]

    print(f"粗糙度数据网格大小: {len(lats_z0)}x{len(lons_z0)}")

    # 调整经度到0-360范围
    power_plants['Longitude'] = power_plants['Longitude'] % 360

    # 构建粗糙度数据的KD树
    lon_grid, lat_grid = np.meshgrid(lons_z0, lats_z0)
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
    tree = cKDTree(grid_points)

    print("网格点构建完成，开始查找最近点...")

    # 查询每个位置的最近网格点
    query_points = power_plants[['Latitude', 'Longitude']].values
    _, indices = tree.query(query_points)

    # 确保索引是整数类型
    lat_indices = (indices // len(lons_z0)).astype(np.int32)
    lon_indices = (indices % len(lons_z0)).astype(np.int32)

    # 验证索引是否在有效范围内
    if (lat_indices >= len(lats_z0)).any() or (lon_indices >= len(lons_z0)).any():
        raise ValueError("计算出的索引超出网格范围")

    print(f"索引范围检查完成，lat_idx: [{lat_indices.min()}, {lat_indices.max()}], "
          f"lon_idx: [{lon_indices.min()}, {lon_indices.max()}]")

    power_plants['z0_lat_idx'] = lat_indices
    power_plants['z0_lon_idx'] = lon_indices

    # 加载所有粗糙度数据
    z0_monthly = {}
    for file in tqdm(z0_files, desc='加载粗糙度数据'):
        try:
            year = int(file[:4])
            month = int(file[4:6])
            if not (2010 <= year <= 2020):
                continue

            with nc.Dataset(os.path.join(z0_dir, file), 'r') as ds:
                z0 = ds.variables['Monthly_z0m_25km'][:]
                z0 = np.ma.masked_equal(z0, 0)  # 处理缺失值
                z0_monthly[(year, month)] = z0
        except Exception as e:
            print(f"加载文件{file}出错: {str(e)}")
            continue

    print(f"成功加载了{len(z0_monthly)}个月份的粗糙度数据")

    # 计算每个位置的平均Z0
    avg_z0 = np.zeros(len(power_plants))
    for i in tqdm(range(len(power_plants)), desc='计算平均Z0'):
        lat_idx = int(power_plants.iloc[i]['z0_lat_idx'])
        lon_idx = int(power_plants.iloc[i]['z0_lon_idx'])

        z0_values = []
        for z0 in z0_monthly.values():
            try:
                val = z0[lat_idx, lon_idx]
                if not np.ma.is_masked(val) and val > 0:
                    z0_values.append(val)
            except IndexError:
                print(f"索引错误: lat_idx={lat_idx}, lon_idx={lon_idx}, z0.shape={z0.shape}")
                continue

        # 计算该坐标点的平均值，如果没有任何有效值，则使用所有坐标点的平均值
        if z0_values:
            avg_z0[i] = np.mean(z0_values)
        else:
            # 计算所有坐标点的平均值
            all_values = [z0[lat_idx, lon_idx] for z0 in z0_monthly.values() if not np.ma.is_masked(z0[lat_idx, lon_idx])]
            avg_z0[i] = np.mean(all_values) if all_values else 0.03

    power_plants['avg_z0'] = avg_z0
    return z0_monthly


def find_nearest_grid_points_vectorized(lats, lons, nc_lats, nc_lons):
    """向量化方式找到最近的网格点"""
    # 调整经度到0-360范围
    lons = np.where(lons < 0, lons + 360, lons)
    # 构建网格点
    lon_grid, lat_grid = np.meshgrid(nc_lons, nc_lats)
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
    tree = cKDTree(grid_points)
    # 查询点
    query_points = np.column_stack((lats, lons))
    _, indices = tree.query(query_points)
    lat_idx = indices // len(nc_lons)
    lon_idx = indices % len(nc_lons)
    return lat_idx, lon_idx


def process_yearly_data(year, nc_files, nc_dir, power_plants, z0_monthly):
    """处理特定年份的数据"""
    print(f"\n开始处理{year}年的数据...")

    total_valid_hours = np.zeros(len(power_plants))
    total_hours = 0

    # 预处理网格点索引（第一个文件）
    with nc.Dataset(os.path.join(nc_dir, nc_files[0]), 'r') as ds:
        lats = ds.variables['lat'][:]
        lons = ds.variables['lon'][:]
        # 调整经度到0-360范围
        lons = lons % 360
        lat_idx, lon_idx = find_nearest_grid_points_vectorized(
            power_plants['Latitude'].values,
            power_plants['Longitude'].values,
            lats, lons
        )

    print(f"已确定网格点索引，开始处理{len(nc_files)}个文件")

    for file in nc_files:
        try:
            # 解析当前文件年月
            year_file = int(file.split('-')[0])
            month_file = int(file.split('-')[1].split('_')[0])

            # 确定Z0值
            z0_values = None
            if (year_file, month_file) in z0_monthly:
                z0_data = z0_monthly[(year_file, month_file)]
                z0_values = z0_data[
                    power_plants['z0_lat_idx'].values,
                    power_plants['z0_lon_idx'].values
                ]
                z0_values = np.ma.filled(z0_values, np.nan)
                valid = ~np.isnan(z0_values) & (z0_values > 0)
                z0 = np.where(valid, z0_values, power_plants['avg_z0'].values)
            else:
                # 如果没有对应月份的Z0数据，使用该地点的平均值
                z0 = power_plants['avg_z0'].values

            # 计算修正系数
            Z1, Z2 = 10.0, 109.0
            coeff = np.log(Z2 / z0) / np.log(Z1 / z0)

            with nc.Dataset(os.path.join(nc_dir, file), 'r') as ds:
                times = ds.variables['time'][:]
                total_hours += len(times)

                # 批量处理数据
                batch_size = 50
                for batch in tqdm(range(0, len(times), batch_size), desc=f'处理{file}'):
                    end = min(batch + batch_size, len(times))
                    wind_speed = ds.variables['wind_speed'][batch:end]

                    # 提取所有位置的风速并修正
                    wind_speed = wind_speed[:, lat_idx, lon_idx]
                    wind_speed = wind_speed * coeff

                    # 统计有效小时
                    valid = (wind_speed >= 5) & (wind_speed <= 20)
                    total_valid_hours += np.sum(valid, axis=0)

        except Exception as e:
            print(f"处理文件{file}时出错: {str(e)}")
            continue

    print(f"{year}年数据处理完成，总小时数: {total_hours}")

    return pd.DataFrame({
        'Latitude': power_plants['Latitude'],
        'Longitude': power_plants['Longitude'],
        'valid_hours': total_valid_hours,
        'total_hours': total_hours,
        'valid_ratio': total_valid_hours / total_hours,
        'year': year,
        'avg_z0': power_plants['avg_z0']  # 添加avg_z0列
    })


def main():
    """主函数"""
    print("开始数据处理...")

    # 设置路径
    nc_dir = r'G:\windspeed'
    z0_dir = r'G:\monthly aerodynamic roughness length dataset'
    output_dir = r'E:\PythonProjiects\Data_of_energy_competition\output'
    wind_loc_file = r'E:\PythonProjiects\Data_of_energy_competition\filtered_wind_farm.xlsx'

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 加载风电场位置
        print("加载风电场位置数据...")
        power_plants = load_wind_power_locations(wind_loc_file)
        print(f"成功加载{len(power_plants)}个风电场位置")

        # 预处理粗糙度数据
        print("\n开始预处理粗糙度数据...")
        z0_monthly = preprocess_roughness_data(z0_dir, power_plants)

        # 检查数据完整性
        print("\n检查数据完整性...")
        completeness_df, files_by_year = check_data_completeness(nc_dir)
        print("数据完整性检查结果:")
        print(completeness_df)

        # 处理每年数据
        yearly_results = []
        for year, files in files_by_year.items():
            try:
                print(f'\n处理年份: {year}')
                df = process_yearly_data(year, files, nc_dir, power_plants, z0_monthly)
                output_file = os.path.join(output_dir, f'result_{year}.csv')
                df.to_csv(output_file, index=False)
                print(f"已保存{year}年结果到: {output_file}")

                yearly_results.append({
                    'year': year,
                    'avg_ratio': df['valid_ratio'].mean()
                })
            except Exception as e:
                print(f"处理{year}年数据时出错: {str(e)}")
                continue

        # 保存汇总结果
        summary_file = os.path.join(output_dir, 'summary.csv')
        pd.DataFrame(yearly_results).to_csv(summary_file, index=False)
        print(f"\n已保存汇总结果到: {summary_file}")

    except Exception as e:
        print(f"程序执行出错: {str(e)}")
        raise

    print("\n数据处理完成!")


if __name__ == '__main__':
    main()