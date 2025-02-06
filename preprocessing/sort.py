#!/usr/bin/env python3
import sys
import os
import eccodes


def read_grib_messages(input_path):
    """
    读取 GRIB 文件中的所有消息，并返回列表，每个元素为 (dataDate, stepRange, message_id)
    """
    messages = []
    with open(input_path, 'rb') as f:
        while True:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                break  # 文件结束
            try:
                # 获取 dataDate 和 stepRange 信息，注意可能需要根据实际情况转换数据类型
                data_date = eccodes.codes_get_long(gid, 'dataDate')
                # stepRange 有可能是以字符串方式存储（比如 "24"），这里尝试转为整数
                step_range_raw = eccodes.codes_get_string(gid, 'stepRange')
                try:
                    step_range = int(step_range_raw)
                except Exception:
                    # 如果转换失败，可以直接使用原始值
                    step_range = step_range_raw

                messages.append((data_date, step_range, gid))
            except Exception as e:
                print(f"读取消息时出错: {e}", file=sys.stderr)
                eccodes.codes_release(gid)
    return messages


def write_grib_messages(messages, output_path):
    """
    将排序后的 GRIB 消息写入到输出文件
    """
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_path, 'wb') as f_out:
        for (data_date, step_range, gid) in messages:
            try:
                eccodes.codes_write(gid, f_out)
            except Exception as e:
                print(f"写入消息时出错: {e}", file=sys.stderr)
            finally:
                eccodes.codes_release(gid)


def main():
    # 固定输入文件路径
    input_path = r'M:\era5\output\2020-08.grib'

    # 获取输入文件所在目录，并在该目录下创建 Sorted 文件夹
    input_dir = os.path.dirname(input_path)
    sorted_dir = os.path.join(input_dir, 'Sorted')
    # 构造输出文件路径，例如在 Sorted 文件夹下输出 sorted_1990-01.grib
    output_filename = 'sorted_2020-08.grib'
    output_path = os.path.join(sorted_dir, output_filename)

    # 读取所有消息
    messages = read_grib_messages(input_path)
    print(f"读取到 {len(messages)} 条消息")

    # 按 dataDate 和 stepRange 排序
    messages.sort(key=lambda item: (item[0], item[1]))
    print("消息已按时间排序")

    # 写入排序后的 GRIB 文件
    write_grib_messages(messages, output_path)
    print(f"排序后的文件已保存为 {output_path}")


if __name__ == '__main__':
    main()
