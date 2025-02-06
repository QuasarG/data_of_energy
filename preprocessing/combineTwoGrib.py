import os
import sys
import eccodes
import logging

# 配置日志
logging.basicConfig(
    filename='merge_log.txt',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def merge_grib_files(source_file, dest_file):
    """
    读取 source_file 中的 GRIB 消息，并将其追加写入到 dest_file 中。
    :param source_file: 源 GRIB 文件（只读二进制模式）
    :param dest_file: 目标 GRIB 文件（追加二进制模式）
    """
    try:
        with open(source_file, 'rb') as sf, open(dest_file, 'ab') as df:
            logging.info("开始合并文件：%s 到 %s", source_file, dest_file)
            while True:
                msg = eccodes.codes_grib_new_from_file(sf)
                if not msg:
                    break
                try:
                    eccodes.codes_write(msg, df)
                finally:
                    eccodes.codes_release(msg)
            logging.info("合并文件 %s 完成", source_file)
    except Exception as e:
        logging.error("合并文件 %s 时发生错误: %s", source_file, str(e))
        raise


def main():
    if len(sys.argv) != 4:
        print("用法: python merge_grib.py <input1.grib> <input2.grib> <output.grib>")
        sys.exit(1)

    input1 = sys.argv[1]
    input2 = sys.argv[2]
    output = sys.argv[3]

    try:
        # 强制创建新输出文件（如果存在则删除）
        if os.path.exists(output):
            os.remove(output)

        # 合并两个输入文件到新输出文件
        merge_grib_files(input1, output)
        merge_grib_files(input2, output)

        logging.info("合并完成：%s + %s → %s", input1, input2, output)
        print(f"合并成功 → {output}")
    except Exception as e:
        logging.error("合并失败: %s", str(e))
        print("错误详情请查看 merge_log.txt")
        sys.exit(1)

if __name__ == "__main__":
    main()
