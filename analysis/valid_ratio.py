# 检测invalid_ratio列

import pandas as pd
import numpy as np

# 读取invalid_ratio列
def detect_invalid_ratio(file_path):
    df = pd.read_csv(file_path)
    invalid_ratio = df['invalid_ratio']

    # 统计valid_ratio
    valid_ratio = 1.00 - invalid_ratio

    # 统计有效率大于特定比例的行所占比例
    threshold = 0.285 # 28.5%
    valid_ratio_above_threshold = valid_ratio[valid_ratio > threshold].count() / len(valid_ratio)

    return valid_ratio_above_threshold

