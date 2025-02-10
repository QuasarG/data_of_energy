import numpy as np
import pygrib
import xarray as xr

def calculate_wind_speed_with_pygrib(input_file, output_nc):
    """
    使用 pygrib 计算风速并保存为 NetCDF 文件。
    参数:
        input_file (str): GRIB 文件路径。
        output_nc (str): 输出的 NetCDF 文件路径。
    """
    try:
        # 打开 GRIB 文件
        print("正在加载 GRIB 文件...")
        grbs = pygrib.open(input_file)

        # 初始化存储数据
        wind_speed_data = []
        time_indices = []
        step_indices = []
        latitude = None
        longitude = None

        for i in range(1, grbs.messages + 1):  # 遍历所有消息
            try:
                grb = grbs.message(i)  # 读取第 i 条消息
                # 获取时间步和步长
                time_idx = int(grb.dataDate)
                step_idx = int(grb.endStep)
                print(f"读取到消息 - 时间步: {time_idx}, 步长: {step_idx}")

                # 检查是否是该月 1 日的数据
                if time_idx >= 20020601 and time_idx < 20020701:
                    # 获取变量名
                    variable = grb.name
                    print(f"变量名: {variable}")

                    if variable in ["10 metre U wind component", "10 metre V wind component"]:
                        # 提取纬度和经度信息（仅提取一次）
                        if latitude is None:
                            latitude, longitude = grb.latlons()
                            print("已提取纬度和经度信息。")

                        # 提取 u10 和 v10 数据
                        data, _, _ = grb.data()
                        data[data == 9999.0] = 0.0  # 替换异常值

                        if variable == "10 metre U wind component":
                            u10 = data
                            print("已提取 u10 数据。")
                        elif variable == "10 metre V wind component":
                            v10 = data
                            print("已提取 v10 数据。")

                            # 计算风速
                            try:
                                wind_speed = np.sqrt(u10**2 + v10**2)
                                print("已计算风速。")
                            except Exception as e:
                                print(f"计算风速时出错：{str(e)}")
                                continue

                            # 将风速数据添加到列表中
                            wind_speed_data.append(wind_speed)
                            time_indices.append(time_idx)
                            step_indices.append(step_idx)

            except Exception as e:
                print(f"处理第 {i} 条消息时出错：{str(e)}")
                continue

        # 检查是否读取到数据
        if not wind_speed_data:
            print("未找到任何有效数据，程序退出。")
            return

        # 将风速数据转换为 numpy 数组
        wind_speed_data = np.array(wind_speed_data)
        unique_lat = np.unique(latitude)
        unique_lon = np.unique(longitude)

        # 创建 xarray Dataset
        new_ds = xr.Dataset(
            {
                "wind_speed": (["time", "latitude", "longitude"], wind_speed_data),
            },
            coords={
                "time": time_indices,
                "latitude": unique_lat,
                "longitude": unique_lon,
            }
        )

        # 保存为 NetCDF 文件
        new_ds.to_netcdf(output_nc)
        print(f"风速数据已保存至：{output_nc}")

    except Exception as e:
        print(f"处理 GRIB 文件时出错：{str(e)}")

# 输入 GRIB 文件路径
input_file = r"E:\PythonProjiects\Data_of_energy_competition\1990-01.grib"
# 输出 NetCDF 文件路径
output_nc = r"E:\PythonProjiects\Data_of_energy_competition\1990-01_wind_speed.nc"
# 计算并保存风速
calculate_wind_speed_with_pygrib(input_file, output_nc)