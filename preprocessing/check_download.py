import os
import re
import pygrib
from datetime import datetime
import calendar
from collections import defaultdict
from typing import List, Set, Dict, Tuple, Optional

# 定义类型别名，使代码更易读和维护
YearMonth = Tuple[int, int]  # 表示 (年, 月) 的元组
DaySet = Set[int]  # 表示天数的集合


class MonthStatus:
    """
    用于跟踪每个月份的数据状态

    Attributes:
        has_full_month (bool): 是否存在包含整月数据的文件
        individual_days (Set[int]): 存储单独的日期文件的集合
        is_grib_complete (bool): 是否是 .grib 文件并且日期完整
    """

    def __init__(self):
        self.has_full_month: bool = False
        self.individual_days: DaySet = set()
        self.is_grib_complete: bool = False

    def is_complete(self) -> bool:
        """
        判断这个月的数据是否完整
        Returns:
            bool: 如果有整月数据文件或所有天数的单独文件都存在，返回True
        """
        return self.has_full_month or self.is_grib_complete

    def get_missing_days(self, year: int, month: int) -> List[int]:
        """
        计算缺失的日期

        Args:
            year (int): 年份
            month (int): 月份

        Returns:
            List[int]: 缺失日期的列表，如果月份完整则返回空列表
        """
        if self.has_full_month or self.is_grib_complete:
            return []
        days_in_month = calendar.monthrange(year, month)[1]
        return sorted(set(range(1, days_in_month + 1)) - self.individual_days)


def parse_filename(filename: str, directory: str) -> Tuple[Optional[int], Optional[int], Optional[int], bool, bool]:
    """
    解析文件名中的日期信息

    支持的文件名格式：
    - xxxx-xx_partial.zip: 包含整月数据
    - xxxx-xx.grib: 包含整月数据
    - xxxx-xx-xx.zip: 包含单日数据

    Args:
        filename (str): 需要解析的文件名
        directory (str): 文件所在目录

    Returns:
        Tuple[Optional[int], Optional[int], Optional[int], bool, bool]:
        返回 (年, 月, 日, 是否为整月数据, 是否为 .grib 文件且完整)
    """
    # 匹配整月数据的文件名模式
    full_month_pattern1 = r'(\d{4})-(\d{2})_partial\.zip'
    full_month_pattern2 = r'(\d{4})-(\d{2})\.grib'

    # 匹配单日数据的文件名模式
    single_day_pattern = r'(\d{4})-(\d{2})-(\d{2})\.zip'

    # 检查是否是.grib文件（无论是否完整）
    if re.match(full_month_pattern2, filename):
        year, month = map(int, re.match(full_month_pattern2, filename).groups())
        is_grib_file = directory.lower() == r'm:\era5\output'  # 仅标记特定目录的.grib文件
        return year, month, None, False, is_grib_file  # 第五个参数改为 is_grib_file

    # 首先检查是否是整月数据文件
    match = re.match(full_month_pattern1, filename) or re.match(full_month_pattern2, filename)
    if match:
        year, month = map(int, match.groups())
        is_grib_complete = directory.lower() == r'm:\era5\output' and filename.endswith('.grib')
        return year, month, None, True, is_grib_complete

    # 然后检查是否是单日数据文件
    match = re.match(single_day_pattern, filename)
    if match:
        year, month, day = map(int, match.groups())
        return year, month, day, False, False

    return None, None, None, False, False


def analyze_directories(directories: List[str]) -> Dict[YearMonth, MonthStatus]:
    """
    分析多个目录中的所有文件，收集月份状态信息（已修改处理.grib的逻辑）
    """
    month_statuses: Dict[YearMonth, MonthStatus] = defaultdict(MonthStatus)

    for directory_path in directories:
        if not os.path.exists(directory_path):
            print(f"警告：目录 {directory_path} 不存在，已跳过")
            continue

        print(f"正在分析目录: {directory_path}")
        for filename in os.listdir(directory_path):
            year, month, day, is_full_month, is_grib_file = parse_filename(filename, directory_path)

            if not (year and month):
                continue  # 跳过无法解析的文件

            status = month_statuses[(year, month)]

            if is_full_month:
                print(f"  找到完整月份数据文件：{filename}")
                status.has_full_month = True

            elif is_grib_file:  # 修改此处变量名以更清晰
                print(f"  找到 .grib 文件：{filename}")

                # 检查完整性并获取包含的日期
                is_complete, days_in_grib = check_grib_complete(directory_path, filename, year, month)

                # 无论是否完整，都记录包含的日期
                status.individual_days.update(days_in_grib)

                if is_complete:
                    status.is_grib_complete = True
                else:
                    print(f"  .grib 文件 {filename} 不完整，但已记录现有日期")

            elif day:
                print(f"  找到单日数据文件：{filename}，日期 {year}-{month}-{day}")
                status.individual_days.add(day)

    return month_statuses


def check_grib_complete(directory: str, filename: str, year: int, month: int) -> Tuple[bool, Set[int]]:
    """
    检查 .grib 文件是否包含当前月的所有日期，并返回包含的日期集合

    Args:
        directory (str): 文件所在的目录
        filename (str): 文件名
        year (int): 年份
        month (int): 月份

    Returns:
        Tuple[bool, Set[int]]: (是否完整, 包含的日期集合)
    """
    days_in_month = calendar.monthrange(year, month)[1]
    expected_days = set(range(1, days_in_month + 1))
    grib_days = set()

    try:
        grbs = pygrib.open(os.path.join(directory, filename))
        print(f"  文件 {filename} 包含以下日期（消息总数：{len(grbs)} 条）：")

        for grb in grbs:
            date = grb.validDate
            if date.year == year and date.month == month:
                grib_days.add(date.day)
                print(f"    消息日期: {date.strftime('%Y-%m-%d')}")

        print(f"  .grib 文件 {filename} 中包含的日期：{sorted(grib_days)}")
        is_complete = grib_days == expected_days

        if is_complete:
            print(f"  .grib 文件 {filename} 完整，包含了所有 {len(expected_days)} 天")
        else:
            missing_days = expected_days - grib_days
            print(f"  .grib 文件 {filename} 缺少日期 {sorted(missing_days)}")

        return is_complete, grib_days

    except Exception as e:
        print(f"  错误读取 .grib 文件 {filename}: {e}")
        return False, grib_days


def find_missing_months(
        month_statuses: Dict[YearMonth, MonthStatus],
        start_year: int = 1990,
        end_year: int = 2024
) -> Tuple[Set[YearMonth], Dict[YearMonth, List[int]]]:
    """
    找出指定年份范围内缺失的月份和不完整的月份

    Args:
        month_statuses: 月份状态字典
        start_year: 起始年份
        end_year: 结束年份

    Returns:
        Tuple[Set[YearMonth], Dict[YearMonth, List[int]]]:
        返回 (完全缺失的月份集合, 不完整月份及其缺失天数的字典)
    """
    completely_missing: Set[YearMonth] = set()
    incomplete_months: Dict[YearMonth, List[int]] = {}

    current_year = datetime.now().year
    current_month = datetime.now().month

    print("开始检查缺失和不完整的月份...")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # 跳过未来的月份
            if year > current_year or (year == current_year and month > current_month):
                continue

            month_key = (year, month)
            if month_key not in month_statuses:
                print(f"  完全缺失的月份：{year}-{month:02d}")
                completely_missing.add(month_key)
            else:
                status = month_statuses[month_key]
                if not status.is_complete():
                    missing_days = status.get_missing_days(year, month)
                    if missing_days:
                        print(f"  {year}-{month:02d} 不完整，缺少日期 {missing_days}")
                        incomplete_months[month_key] = missing_days

    return completely_missing, incomplete_months


def save_results(
        output_file: str,
        month_statuses: Dict[YearMonth, MonthStatus],
        completely_missing: Set[YearMonth],
        incomplete_months: Dict[YearMonth, List[int]],
        directories: List[str]
) -> None:
    """
    将分析结果保存到文本文件中

    Args:
        output_file: 输出文件路径
        month_statuses: 月份状态字典
        completely_missing: 完全缺失的月份集合
        incomplete_months: 不完整月份字典
        directories: 分析的目录列表
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入分析的目录信息
        f.write("分析的目录列表：\n")
        for directory in directories:
            f.write(f"- {directory}\n")

        # 写入完全缺失的月份
        f.write("\n完全缺失的月份：\n")
        if completely_missing:
            for year, month in sorted(completely_missing):
                f.write(f"{year}-{month:02d}\n")
        else:
            f.write("没有完全缺失的月份\n")

        # 写入不完整的月份信息
        f.write("\n不完整的月份（缺少的天数）：\n")
        if incomplete_months:
            for (year, month), missing_days in sorted(incomplete_months.items()):
                f.write(f"{year}-{month:02d}: 缺少日期 {', '.join(map(str, missing_days))}\n")
        else:
            f.write("没有不完整的月份\n")

        # 写入总结信息
        f.write(f"\n分析总结：\n")
        f.write(f"完全缺失的月份数量: {len(completely_missing)}\n")
        f.write(f"不完整月份数量: {len(incomplete_months)}\n")


def main(directories: List[str], output_file: str = "date_analysis_results.txt") -> None:
    """
    主函数：协调整个分析过程

    Args:
        directories: 需要分析的目录路径列表
        output_file: 输出结果文件的路径
    """
    print("开始分析目录...")
    month_statuses = analyze_directories(directories)

    print("检查缺失和不完整的月份...")
    completely_missing, incomplete_months = find_missing_months(month_statuses)

    print("保存分析结果...")
    save_results(output_file, month_statuses, completely_missing, incomplete_months, directories)

    print(f"分析完成！结果已保存到 {output_file}")
    print(f"完全缺失的月份数量: {len(completely_missing)}")
    print(f"不完整月份数量: {len(incomplete_months)}")


if __name__ == "__main__":
    directories_to_analyze = [r'G:\era5', r'F:\data_from_era5', r'M:\era5\output', r'M:\era5']
    # 运行分析
    main(directories_to_analyze)
