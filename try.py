import os

# 指定目录路径
directory = 'M:\era5\output'

# 遍历目录中的所有文件
for filename in os.listdir(directory):
    # 检查文件是否以 .grib_cache 结尾
    if filename.endswith('.grib_cache'):
        # 构建新文件名
        new_filename = filename[:-len('.grib_cache')] + '.grib'
        # 构建完整的旧文件路径
        old_file = os.path.join(directory, filename)
        # 构建完整的新文件路径
        new_file = os.path.join(directory, new_filename)
        # 重命名文件
        os.rename(old_file, new_file)
        print(f'Renamed: {old_file} to {new_file}')
