import os
import glob
import zipfile
import re
import eccodes
from datetime import datetime
from itertools import groupby
import logging
import time
import calendar

# 配置 logging
logging.basicConfig(
    filename='process_log.txt',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def merge_to_monthly(daily_file, monthly_file):
    """
    将每日 GRIB 文件合并到月文件中（直接追加，不覆盖已有数据）
    :param daily_file: 每日 GRIB 文件路径
    :param monthly_file: 月文件路径
    """
    try:
        with open(daily_file, 'rb') as df, open(monthly_file, 'ab') as mf:
            while True:
                msg = eccodes.codes_grib_new_from_file(df)
                if not msg:
                    break
                try:
                    # 直接追加写入到月文件
                    eccodes.codes_write(msg, mf)
                finally:
                    eccodes.codes_release(msg)
    except Exception as e:
        logging.error(f"[错误] 合并文件时发生错误: {str(e)}")
        print(f"[错误] 合并文件时发生错误: {str(e)}")
        raise

def process_daily_zip_files(zip_dir, output_dir):
    """
    处理每日 ZIP 文件，按月份分组，
    每个文件解压、重命名、合并到对应月份的 GRIB 文件中，
    解压后暂不删除 ZIP 文件，处理完一个月后自动删除本月已处理成功的 ZIP 文件，
    并自动处理下一个月份。
    在开始处理每个新月份前停顿3秒，期间可通过 Ctrl+C 中断程序。
    同时对比该月预期日期与实际文件数，记录缺失日期。
    """
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"创建或检查输出目录: {output_dir}")

    # 获取并验证 ZIP 文件
    zip_files = glob.glob(os.path.join(zip_dir, "*.zip"))
    valid_zips = []
    for zf in zip_files:
        basename = os.path.basename(zf)
        if re.match(r'^\d{4}-\d{2}-\d{2}\.zip$', basename):
            valid_zips.append(zf)
            logging.info(f"找到有效 ZIP 文件: {basename}")
        else:
            logging.warning(f"跳过不符合命名规范的文件: {basename}")
            print(f"[跳过] 不符合命名规范的文件: {basename}")

    # 按日期排序并按月份分组（以 "YYYY-MM" 为键）
    valid_zips.sort(key=lambda x: os.path.basename(x).split('.')[0])
    month_groups = {}
    for month, group in groupby(valid_zips, key=lambda x: os.path.basename(x)[:7]):
        group_list = list(group)
        if group_list:
            month_groups[month] = group_list

    # 按月份依次处理
    for month, files in month_groups.items():
        # 在开始处理每个新月份前停顿3秒，期间可通过 Ctrl+C 中断程序
        print(f"\n即将开始处理 {month} 的数据，等待3秒……（Ctrl+C 可中断）")
        time.sleep(3)

        # 对比该月预期日期与实际文件数（根据文件名）
        try:
            year_num, month_num = map(int, month.split('-'))
        except Exception as e:
            logging.error(f"解析月份 {month} 时出错: {str(e)}")
            continue

        # 计算该月实际天数
        _, last_day = calendar.monthrange(year_num, month_num)
        expected_dates = {f"{month}-{day:02d}" for day in range(1, last_day+1)}
        # 从文件列表中提取实际日期（文件名格式：YYYY-MM-DD.zip）
        actual_dates = {os.path.basename(x).split('.')[0] for x in files}
        missing_dates = sorted(expected_dates - actual_dates)
        if missing_dates:
            logging.warning(f"月份 {month} 缺失数据日期：{missing_dates}")
            print(f"[警告] 月份 {month} 缺失数据日期：{missing_dates}")
        else:
            logging.info(f"月份 {month} 数据完整。")
            print(f"月份 {month} 数据完整。")

        logging.info(f"\n开始处理月份: {month}")
        print(f"\n{'=' * 40}")
        print(f"开始处理月份: {month}")

        monthly_file = os.path.join(output_dir, f"{month}.grib_cache")
        # 采用追加模式写入，不删除已有的月文件

        total_days = len(files)
        processed_days = 0
        # 记录本月处理成功的 ZIP 文件路径，后续自动删除
        zips_to_delete = []

        # 每批次（每 3 天）处理一次
        for i in range(0, total_days, 3):
            batch = files[i:i + 3]
            temp_files = []  # 用于记录当前批次解压后生成的临时 GRIB 文件

            for zip_file in batch:
                file_error = False  # 单个文件处理错误标志
                date_str = os.path.basename(zip_file).split('.')[0]
                temp_dir = os.path.join(output_dir, f"temp_{date_str}")
                try:
                    logging.info(f"\n正在处理 {date_str}.zip")
                    print(f"\n[处理] 正在处理 {date_str}.zip")

                    # 创建临时解压目录
                    os.makedirs(temp_dir, exist_ok=True)
                    logging.info(f"创建临时解压目录: {temp_dir}")

                    # 解压 ZIP 文件
                    with zipfile.ZipFile(zip_file, 'r') as zf:
                        zf.extractall(temp_dir)

                    # 验证解压结果：期望只有一个文件
                    extracted = glob.glob(os.path.join(temp_dir, "*"))
                    if len(extracted) != 1:
                        logging.warning(f"{date_str}.zip 包含多个文件，跳过处理")
                        print(f"[警告] {date_str}.zip 包含多个文件，跳过处理")
                        file_error = True
                        continue

                    # 重命名并移动文件到输出目录
                    grib_path = os.path.join(output_dir, f"{date_str}.grib_cache")
                    os.rename(extracted[0], grib_path)
                    temp_files.append(grib_path)
                    logging.info(f"文件已保存为 {grib_path}")
                    print(f"[成功] 文件已保存为 {grib_path}")

                    # 合并到当前月份的 GRIB 文件
                    logging.info(f"正在合并到月文件 {monthly_file}")
                    print(f"[合并] 正在合并到月文件 {monthly_file}")
                    merge_to_monthly(grib_path, monthly_file)

                    # 如果处理成功，则将 ZIP 文件加入待删除列表
                    if not file_error:
                        zips_to_delete.append(zip_file)

                except Exception as e:
                    file_error = True
                    logging.error(f"处理 {zip_file} 时发生错误: {str(e)}")
                    print(f"[错误] 处理 {zip_file} 时发生错误: {str(e)}")
                finally:
                    # 清理当前 ZIP 文件对应的临时目录
                    if os.path.exists(temp_dir):
                        for root, dirs, files_in_dir in os.walk(temp_dir, topdown=False):
                            for name in files_in_dir:
                                try:
                                    os.remove(os.path.join(root, name))
                                except Exception as e:
                                    logging.error(f"删除文件 {os.path.join(root, name)} 时出错: {str(e)}")
                            for name in dirs:
                                try:
                                    os.rmdir(os.path.join(root, name))
                                except Exception as e:
                                    logging.error(f"删除目录 {os.path.join(root, name)} 时出错: {str(e)}")
                        try:
                            os.rmdir(temp_dir)
                            logging.info(f"已清理临时目录: {temp_dir}")
                        except Exception as e:
                            logging.error(f"删除临时目录 {temp_dir} 时出错: {str(e)}")
                    processed_days += 1

            # 每个批次处理完后，清理本批次产生的临时 GRIB 文件
            if temp_files:
                logging.info("\n正在清理临时 GRIB 文件...")
                print("\n[清理] 正在清理临时 GRIB 文件...")
                for tf in temp_files:
                    if os.path.exists(tf):
                        try:
                            os.remove(tf)
                            logging.info(f"已删除临时文件: {os.path.basename(tf)}")
                            print(f"[清理] 已删除临时文件 {os.path.basename(tf)}")
                        except Exception as e:
                            logging.error(f"删除临时文件 {tf} 时出错: {str(e)}")
                logging.info(f"[完成] 已成功处理 {len(batch)} 天的数据")
                print(f"[完成] 已成功处理 {len(batch)} 天的数据")
            logging.info(f"\n进度: 已完成 {processed_days}/{total_days} 天")
            print(f"\n{'=' * 40}")
            print(f"进度: 已完成 {processed_days}/{total_days} 天")

        # 自动删除本月已处理成功的 ZIP 文件
        for zip_file in zips_to_delete:
            if os.path.exists(zip_file):
                try:
                    os.remove(zip_file)
                    logging.info(f"已删除原始文件: {zip_file}")
                    print(f"[清理] 已删除原始文件 {zip_file}")
                except Exception as e:
                    logging.error(f"删除 ZIP 文件 {zip_file} 时出错: {str(e)}")
        logging.info(f"本月 {month} 的 ZIP 文件已删除")
        print(f"本月 {month} 的 ZIP 文件已删除")

    # 所有月份处理完毕
    logging.info("\n处理流程全部完成！")
    print("\n处理流程全部完成！")

def main():
    zip_directory = r'F:\data_from_era5'
    output_directory = r'M:\era5\output'

    try:
        process_daily_zip_files(zip_directory, output_directory)
    except Exception as e:
        logging.error(f"\n处理过程中发生未捕获的异常: {str(e)}")
        print(f"\n处理过程中发生未捕获的异常: {str(e)}")
    finally:
        # 最终清理所有残留的临时目录
        for temp_dir in glob.glob(os.path.join(output_directory, "temp_*")):
            if os.path.isdir(temp_dir):
                for root, dirs, files_in_dir in os.walk(temp_dir, topdown=False):
                    for name in files_in_dir:
                        try:
                            os.remove(os.path.join(root, name))
                        except Exception as e:
                            logging.error(f"删除文件 {os.path.join(root, name)} 时出错: {str(e)}")
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except Exception as e:
                            logging.error(f"删除目录 {os.path.join(root, name)} 时出错: {str(e)}")
                try:
                    os.rmdir(temp_dir)
                    logging.info(f"已清理最终临时目录: {temp_dir}")
                except Exception as e:
                    logging.error(f"删除临时目录 {temp_dir} 时出错: {str(e)}")

if __name__ == "__main__":
    main()
