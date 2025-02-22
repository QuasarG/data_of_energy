import os
import zipfile
import datetime
import numpy as np
import pygrib
import netCDF4 as nc
from collections import defaultdict
import shutil
from tqdm import tqdm
import logging

# 配置日志记录
logging.basicConfig(
    filename="processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_message(message, level="INFO"):
    """记录日志并打印到控制台"""
    if level == "INFO":
        logging.info(message)
        print(f"[INFO] {message}")
    elif level == "ERROR":
        logging.error(message)
        print(f"[ERROR] {message}")

def get_free_space(directory):
    """获取目录所在磁盘的剩余空间（单位：字节）"""
    if not os.path.exists(directory):
        log_message(f"目录不存在: {directory}", level="ERROR")
        return 0
    total, used, free = shutil.disk_usage(directory)
    return free

def select_output_directory(output_directories, min_free_space_gb=15):
    """选择第一个满足空间要求的输出目录"""
    min_free_space_bytes = min_free_space_gb * 1024**3
    for directory in output_directories:
        free_space = get_free_space(directory)
        if free_space >= min_free_space_bytes:
            log_message(f"选择输出目录: {directory} (剩余空间: {free_space / 1024**3:.2f} GB)")
            return directory
        else:
            log_message(f"跳过输出目录: {directory} (剩余空间不足: {free_space / 1024**3:.2f} GB)")
    log_message("所有输出目录空间不足", level="ERROR")
    return None

def clean_cache_directory(cache_dir):
    """清理缓存目录中的所有文件"""
    if os.path.exists(cache_dir):
        for fname in os.listdir(cache_dir):
            file_path = os.path.join(cache_dir, fname)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                log_message(f"清理缓存文件失败: {file_path} ({str(e)})", level="ERROR")
        log_message(f"缓存目录已清理: {cache_dir}")
    else:
        log_message(f"缓存目录不存在: {cache_dir}", level="ERROR")

def record_processed_month(month, processed_file="processed_months.txt"):
    """记录已处理的月份（不再检查重复）"""
    if not month:
        log_message("无效的月份，无法记录", level="ERROR")
        return
    # 确保文件存在
    if not os.path.exists(processed_file):
        with open(processed_file, "w") as f:
            pass  # 创建空文件
    # 写入新记录
    with open(processed_file, "a") as f:
        f.write(f"{month}\n")
    log_message(f"记录已处理月份: {month}")

def process_zip_file(zip_path, cache_dir):
    """处理ZIP文件，解压并重命名为对应的GRIB文件名"""
    try:
        zip_fname = os.path.basename(zip_path)
        log_message(f"开始处理ZIP文件: {zip_fname}")
        # 验证文件名格式
        if "_partial.zip" not in zip_fname:
            log_message(f"跳过非_partial.zip格式文件: {zip_fname}")
            return None
        # 提取基础名称（xxxx-xx）
        base_name = zip_fname.split("_partial.zip")[0]
        if len(base_name.split("-")) != 2 or not base_name.replace("-", "").isdigit():
            log_message(f"文件名格式错误: {zip_fname}", level="ERROR")
            return None
        # 记录已处理的月份
        record_processed_month(base_name)
        # 生成新文件名
        new_grib_name = f"{base_name}.grib"
        new_grib_path = os.path.join(cache_dir, new_grib_name)
        # 解压并重命名
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            if "data.grib" not in zip_ref.namelist():
                log_message(f"ZIP文件中缺失data.grib: {zip_fname}", level="ERROR")
                return None
            # 解压临时文件
            temp_path = os.path.join(cache_dir, "data.grib")
            zip_ref.extract("data.grib", cache_dir)
            log_message(f"成功解压到临时文件: {temp_path}")
            # 重命名并覆盖已存在的文件
            if os.path.exists(new_grib_path):
                log_message(f"发现已存在文件，执行覆盖: {new_grib_path}")
                os.remove(new_grib_path)
            os.rename(temp_path, new_grib_path)
            log_message(f"重命名为标准文件名: {new_grib_path}")
        return new_grib_path
    except Exception as e:
        log_message(f"处理ZIP文件出错: {str(e)}", level="ERROR")
        return None

def calculate_wind_speed_with_pygrib(input_file, output_dir):
    """处理单个GRIB文件，自动识别各个月份数据，跳过缺失日期，为每个月份生成单独NetCDF文件"""
    try:
        log_message(f"开始处理文件: {input_file}")
        grbs = pygrib.open(input_file)
        log_message(f"成功打开GRIB文件: {os.path.basename(input_file)}")
        # 按月份和时间步组织消息
        messages_by_month = defaultdict(list)
        total_messages = len(grbs)  # 获取总消息数
        for grb in tqdm(grbs, desc=f"处理GRIB消息 [{os.path.basename(input_file)}]", unit="msg"):
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
        if not messages_by_month:
            log_message("未找到U/V分量数据", level="ERROR")
            return
        # 处理每个月份
        for (year, month), msgs in messages_by_month.items():
            log_message(f"开始处理 {year}-{month:02d} 数据...")
            # 按时间步分组
            time_steps = defaultdict(dict)
            for msg in msgs:
                key = (msg['date'], msg['step'])
                if "U wind component" in msg['name']:
                    time_steps[key]['u'] = msg
                else:
                    time_steps[key]['v'] = msg
            # 筛选有效时间步并排序
            valid_steps = [k for k, v in time_steps.items() if 'u' in v and 'v' in v]
            valid_steps.sort(key=lambda x: (x[0], x[1]))
            if not valid_steps:
                log_message(f"跳过 {year}-{month:02d}（无完整数据）", level="ERROR")
                continue
            # 获取网格信息
            sample_msg = grbs.message(time_steps[valid_steps[0]]['u']['message'])
            lats, lons = sample_msg.latlons()
            # 检查是否已存在NetCDF文件
            output_path = os.path.join(output_dir, f"{year}-{month:02d}_wind_speed.nc")
            if os.path.exists(output_path):
                ds = nc.Dataset(output_path, "a")  # 打开现有文件
                time_var = ds.variables["time"]
                ws_var = ds.variables["wind_speed"]
                existing_dates = set(time_var[:])
            else:
                ds = nc.Dataset(output_path, "w")  # 创建新文件
                ds.createDimension("time", None)
                ds.createDimension("lat", lats.shape[0])
                ds.createDimension("lon", lats.shape[1])
                time_var = ds.createVariable("time", "i4", ("time",))
                lat_var = ds.createVariable("lat", "f4", ("lat",))
                lon_var = ds.createVariable("lon", "f4", ("lon",))
                ws_var = ds.createVariable("wind_speed", "f4", ("time", "lat", "lon"),
                                           zlib=True, fill_value=-9999.0)
                time_var.units = "YYYYMMDD"
                lat_var.units = "degrees_north"
                lon_var.units = "degrees_east"
                ws_var.units = "m/s"
                ws_var.long_name = "10m wind speed"
                lat_var[:] = lats[:, 0]
                lon_var[:] = lons[0, :]
                existing_dates = set()
            # 追加数据
            for t_idx, (date, step) in enumerate(valid_steps):
                if date in existing_dates:
                    log_message(f"跳过已存在的日期: {date}")
                    continue
                u_msg = grbs.message(time_steps[(date, step)]['u']['message'])
                v_msg = grbs.message(time_steps[(date, step)]['v']['message'])
                u_data = u_msg.values.astype("f4")
                v_data = v_msg.values.astype("f4")
                valid_mask = (u_data != 9999) & (v_data != 9999)
                u_data[~valid_mask] = 0
                v_data[~valid_mask] = 0
                ws = np.sqrt(u_data ** 2 + v_data ** 2)
                ws[~valid_mask] = -9999.0
                ws_var[len(existing_dates) + t_idx, :, :] = ws
                time_var[len(existing_dates) + t_idx] = date
            ds.close()
            log_message(f"完成保存: {os.path.basename(output_path)}")
    except Exception as e:
        log_message(f"处理出错: {str(e)}", level="ERROR")
        raise
    finally:
        if "grbs" in locals():
            grbs.close()
            log_message("关闭GRIB文件句柄")
        # 处理完成后删除原文件
        if os.path.exists(input_file):
            os.remove(input_file)
            log_message(f"已删除原文件: {os.path.basename(input_file)}")

def process_directory(input_dir, output_directories, cache_dir):
    """处理单个目录的核心逻辑"""
    log_message(f"进入目录处理流程: {input_dir}")
    # 第一阶段：处理所有ZIP文件
    zip_files = [fname for fname in os.listdir(input_dir) if fname.lower().endswith(".zip")]
    for fname in tqdm(zip_files, desc="处理ZIP文件", unit="file"):
        zip_path = os.path.join(input_dir, fname)
        try:
            new_grib = process_zip_file(zip_path, cache_dir)
            if new_grib:
                selected_output_dir = select_output_directory(output_directories)
                if not selected_output_dir:
                    log_message("无法继续处理: 所有输出目录空间不足", level="ERROR")
                    break
                calculate_wind_speed_with_pygrib(new_grib, selected_output_dir)
                os.remove(zip_path)
                log_message(f"已清理ZIP文件: {fname}")
        except Exception as e:
            log_message(f"处理ZIP文件失败: {fname} ({str(e)})", level="ERROR")

    # 第二阶段：处理所有GRIB文件
    grib_files = [fname for fname in os.listdir(input_dir) if fname.lower().endswith((".grib", ".grb", ".grib2"))]
    for fname in tqdm(grib_files, desc="处理GRIB文件", unit="file"):
        grib_path = os.path.join(input_dir, fname)
        try:
            selected_output_dir = select_output_directory(output_directories)
            if not selected_output_dir:
                log_message("无法继续处理: 所有输出目录空间不足", level="ERROR")
                break
            calculate_wind_speed_with_pygrib(grib_path, selected_output_dir)
        except Exception as e:
            log_message(f"处理GRIB文件失败: {fname} ({str(e)})", level="ERROR")
    # 清理缓存目录
    clean_cache_directory(cache_dir)

# 配置参数
input_directories = [
    r"M:\era5\test",
    r"G:\era5",     # 1999-2017年数据
    r"F:\data_from_era5", #1991-1998年数据
    r"M:\era5",     # 2017-2019，以及前面漏了的
    r"M:\era5\output", #按天下的数据
]
output_directories = [
    r"M:\windspeed",
    r"G:\windspeed",
    r"F:\windspeed"
]
cache_directory = r"E:\temp"

# 执行处理流程
for input_dir in input_directories:
    if os.path.isdir(input_dir):
        log_message(f"开始处理主目录: {input_dir}")
        process_directory(input_dir, output_directories, cache_directory)
    else:
        log_message(f"目录不存在: {input_dir}", level="ERROR")