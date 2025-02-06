import eccodes
import numpy as np


def decode_grib(filename, output_filename):
    with open(filename, 'rb') as file:
        message_count = 0  # 初始化消息计数器
        with open(output_filename, 'w') as output_file:  # 打开输出文件
            while True:
                message = eccodes.codes_grib_new_from_file(file)
                if not message:
                    break

                message_count += 1  # 每处理一条消息，计数器加1

                # 检查 messageNumber 是否存在
                if eccodes.codes_is_defined(message, 'messageNumber'):
                    message_header = f"=== 消息 {eccodes.codes_get(message, 'messageNumber')} ===\n"
                else:
                    message_header = "=== 消息 (编号未知) ===\n"

                # 消息头 (Message Header)
                message_header += "消息头:\n"
                message_header += f"   数据来源: {eccodes.codes_get(message, 'centreDescription')}\n"
                message_header += f"   观测时间: {eccodes.codes_get(message, 'dataDate')} {eccodes.codes_get(message, 'dataTime')}\n"
                message_header += f"   预报时间: {eccodes.codes_get(message, 'startStep')} 小时\n"
                message_header += f"   空间分辨率: {eccodes.codes_get(message, 'gridType')}\n"

                # 数据描述 (Data Description)
                message_header += "\n数据描述:\n"
                message_header += f"   数据类型: {eccodes.codes_get(message, 'paramId')}\n"
                message_header += f"   格式: {eccodes.codes_get(message, 'bitsPerValue')} 位/值\n"
                message_header += f"   量程: {eccodes.codes_get(message, 'min'):.2f} 到 {eccodes.codes_get(message, 'max'):.2f}\n"
                message_header += f"   网格类型: {eccodes.codes_get(message, 'gridType')}\n"
                message_header += f"   坐标系统: {eccodes.codes_get(message, 'latitudeOfFirstGridPoint')}," \
                                  f" {eccodes.codes_get(message, 'longitudeOfFirstGridPoint')}\n"

                # 数据段 (Data Section)
                data_section = "\n数据段:\n"
                # 获取经纬度数据
                latitudes = eccodes.codes_get_array(message, 'latitudes')
                longitudes = eccodes.codes_get_array(message, 'longitudes')
                data_section += f"   经度数据: {longitudes[:5]} ... {longitudes[-5:]}\n"
                data_section += f"   纬度数据: {latitudes[:5]} ... {latitudes[-5:]}\n"
                # 获取气象数据值
                values = eccodes.codes_get_array(message, 'values')
                valid_values = [v for v in values if v != 9999]  # 排除无效值 9999

                if len(values) > 0:  # 检查 values 是否为空
                    data_section += f"   气象数据值: {values[:5]} ... {values[-5:]}\n"
                    data_section += f"   气象数据总数: {len(values)}\n"
                else:
                    data_section += "   气象数据值: []\n"
                    data_section += "   气象数据总数: 0\n"

                if len(valid_values) > 0:  # 检查 valid_values 是否为空
                    values_avg = np.mean(valid_values)  # 使用 numpy 计算平均值
                    data_section += f"   有效气象数据平均值: {values_avg:.2f}\n"
                else:
                    data_section += "   有效气象数据平均值: 0.00\n"

                # 写入TXT文件
                output_file.write(message_header + data_section)

                # 释放消息
                eccodes.codes_release(message)

        # 输出消息总数
        print(f"文件 {filename} 中的消息总数: {message_count}")


if __name__ == "__main__":
    # 替换为你的 GRIB 文件路径和输出文件路径
    # decode_grib(r'E:\PythonProjiects\Data_of_energy_competition\zip\output\1990-01-01.grib_cache', '1990-01-01output.txt')
    # decode_grib(r'E:\PythonProjiects\Data_of_energy_competition\zip\output\1990-01.grib_cache', '1990-01output.txt')
    decode_grib(r'M:\era5\output\1990-01.grib', '1990-01.txt')