import os
import numpy as np
import pygrib
import netCDF4 as nc
from datetime import datetime
from collections import defaultdict


def calculate_wind_speed_with_pygrib(input_file, output_dir):
    """
    处理单个GRIB文件，自动识别各个月份数据，跳过缺失日期，为每个月份生成单独NetCDF文件

    参数:
        input_file (str): 输入的GRIB文件路径
        output_dir (str): 输出的NetCDF文件目录
    """
    try:
        print(f"正在处理文件: {input_file}")
        grbs = pygrib.open(input_file)
        print(f"成功打开GRIB文件: {input_file}")

        # 按月份和时间步组织消息
        messages_by_month = defaultdict(list)
        for grb in grbs:
            if grb.name in ["10 metre U wind component", "10 metre V wind component"]:
                data_date = grb.dataDate
                year = data_date // 10000
                month = (data_date // 100) % 100
                messages_by_month[(year, month)].append({
                    'date': data_date,
                    'step': grb.endStep,
                    'name': grb.name,
                    'message': grb.messagenumber
                })
                print(f"已识别信息: 年份={year}, 月份={month}, 名称={grb.name}, 消息号={grb.messagenumber}")

        if not messages_by_month:
            print("未找到U/V分量数据")
            return

        # 处理每个月份
        for (year, month), msgs in messages_by_month.items():
            print(f"处理 {year}-{month:02d} 数据...")

            # 按时间步分组
            time_steps = defaultdict(dict)
            for msg in msgs:
                key = (msg['date'], msg['step'])
                if msg['name'].endswith('U wind component'):
                    time_steps[key]['u'] = msg
                else:
                    time_steps[key]['v'] = msg
                print(f"已分组信息: 日期={msg['date']}, 步长={msg['step']}, 名称={msg['name']}")

            # 筛选有效时间步并排序
            valid_steps = [k for k, v in time_steps.items() if 'u' in v and 'v' in v]
            valid_steps.sort(key=lambda x: (x[0], x[1]))  # 按日期和步长排序
            print(f"有效的时间步: {valid_steps}")

            if not valid_steps:
                print(f"跳过 {year}-{month:02d}（无完整数据）")
                continue

            # 获取网格信息
            sample_msg = grbs.message(time_steps[valid_steps[0]]['u']['message'])
            lats, lons = sample_msg.latlons()
            print(f"成功获取网格信息: 纬度长度={lats.shape[0]}, 经度长度={lats.shape[1]}")

            # 创建输出文件
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{year}-{month:02d}_wind_speed.nc")
            print(f"输出文件路径: {output_path}")

            with nc.Dataset(output_path, 'w') as ds:
                # 定义维度
                ds.createDimension('time', None)
                ds.createDimension('lat', lats.shape[0])
                ds.createDimension('lon', lats.shape[1])
                print(f"成功创建NetCDF文件维度: time, lat, lon")

                # 创建变量
                time_var = ds.createVariable('time', 'i4', ('time',))
                lat_var = ds.createVariable('lat', 'f4', ('lat',))
                lon_var = ds.createVariable('lon', 'f4', ('lon',))
                ws_var = ds.createVariable('wind_speed', 'f4',
                                           ('time', 'lat', 'lon'),
                                           zlib=True, fill_value=-9999.0)
                print(f"成功创建NetCDF文件变量: time, lat, lon, wind_speed")

                # 设置属性
                time_var.units = 'YYYYMMDD'
                lat_var.units = 'degrees_north'
                lon_var.units = 'degrees_east'
                ws_var.units = 'm/s'
                ws_var.long_name = '10m wind speed'
                print(f"成功设置NetCDF变量属性")

                # 写入坐标数据
                lat_var[:] = lats[:, 0]
                lon_var[:] = lons[0, :]
                print(f"成功写入纬度和经度坐标数据")

                # 逐时间步处理
                for t_idx, (date, step) in enumerate(valid_steps):
                    print(f"正在处理时间步: 日期={date}, 步长={step}")
                    # 获取消息数据
                    u_msg = grbs.message(time_steps[(date, step)]['u']['message'])
                    v_msg = grbs.message(time_steps[(date, step)]['v']['message'])

                    u_data = u_msg.values.astype('f4')
                    v_data = v_msg.values.astype('f4')
                    print(f"成功获取U和V分量数据: 日期={date}, 步长={step}")

                    # 处理缺失值
                    valid_mask = (u_data != 9999) & (v_data != 9999)
                    u_data[~valid_mask] = 0
                    v_data[~valid_mask] = 0
                    print(f"成功处理缺失值: 日期={date}, 步长={step}")

                    # 计算风速并写入
                    ws = np.sqrt(u_data ** 2 + v_data ** 2)
                    ws[~valid_mask] = -9999.0
                    ws_var[t_idx, :, :] = ws
                    print(f"成功计算并写入风速数据: 日期={date}, 步长={step}")

                    # 记录时间
                    time_var[t_idx] = date
                    print(f"成功记录时间: 日期={date}, 索引={t_idx}")

                    # 清理内存
                    del u_data, v_data, ws
                    print(f"成功清理内存: 时间步索引={t_idx}")

                print(f"已保存: {output_path}")

    except Exception as e:
        print(f"处理出错: {str(e)}")
        raise
    finally:
        if 'grbs' in locals():
            grbs.close()
            print(f"成功关闭GRIB文件")


# 使用示例
input_file = r"M:\era5\2002-06.grib"
output_dir = r"E:\PythonProjiects\Data_of_energy_competition"
calculate_wind_speed_with_pygrib(input_file, output_dir)
