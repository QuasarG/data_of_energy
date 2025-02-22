import os
import re
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, List, Dict

def generate_date_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime('%Y-%m')
        # 移动到下个月的第一天
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

def analyze_files(directory) -> Tuple[List[str], List[Tuple[str, float, float]]]: 
    # 创建完整的日期范围
    start_date = datetime(1990, 1, 1)
    end_date = datetime(2024, 12, 1)
    expected_dates = set(generate_date_range(start_date, end_date))
    
    # 获取目录中的所有文件
    existing_dates = set()
    file_sizes: Dict[str, float] = {}
    pattern = r'(\d{4}-\d{2})_wind_speed\.nc'
    
    for filename in os.listdir(directory):
        if filename.endswith('_wind_speed.nc'):
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                existing_dates.add(date_str)
                # 获取文件大小（以MB为单位）
                file_path = os.path.join(directory, filename)
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
                file_sizes[filename] = file_size
    
    # 找出缺失的日期
    missing_dates = sorted(list(expected_dates - existing_dates))
    
    # 分析文件大小
    if file_sizes:
        sizes = list(file_sizes.values())
        mean_size = np.mean(sizes)
        std_size = np.std(sizes)
        
        # 找出大小异常的文件（超过平均值正负2个标准差）
        anomalous_files = []
        for filename, size in file_sizes.items():
            z_score = abs(size - mean_size) / std_size
            if z_score > 2:  # 使用2个标准差作为阈值
                deviation_percent = ((size - mean_size) / mean_size) * 100
                anomalous_files.append((filename, size, deviation_percent))
        
        return missing_dates, sorted(anomalous_files, key=lambda x: abs(x[2]), reverse=True)
    
    return missing_dates, []

def main():
    directory = r"G:\windspeed"
    
    if not os.path.exists(directory):
        print("错误：目录不存在！")
        return
    
    missing_dates, anomalous_files = analyze_files(directory)
    
    if not missing_dates:
        print("没有缺失的日期！所有月份的数据都存在。")
    else:
        print("\n缺失的月份如下：")
        for date in missing_dates:
            print(date)
        print(f"\n总共缺失 {len(missing_dates)} 个月份的数据")
    
    if anomalous_files:
        print("\n大小异常的文件如下：")
        for filename, size, deviation in anomalous_files:
            print(f"{filename}: {size:.2f}MB (偏离均值 {deviation:+.2f}%)")

if __name__ == "__main__":
    main()
