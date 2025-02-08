import os
import subprocess

def convert_grib_to_nc(grib_file: str, nc_file: str) -> None:
    """
    使用 wgrib 将 GRIB1 文件转换为 NetCDF 格式，并捕获错误信息
    """
    command = f"wgrib {grib_file} -netcdf {nc_file}"
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"成功转换 {grib_file} 到 {nc_file}")
    except subprocess.CalledProcessError as e:
        print(f"转换失败: {grib_file} -> {nc_file}")
        print(f"错误输出：{e.stderr}")
        print(f"标准输出：{e.stdout}")
        print(f"返回代码：{e.returncode}")


def batch_convert_gribs(grib_dir: str, output_dir: str) -> None:
    """
    批量转换目录中的所有 GRIB1 文件到 NetCDF 格式
    """
    # 获取 GRIB 文件列表
    grib_files = [f for f in os.listdir(grib_dir) if f.endswith('.grib')]

    # 创建输出目录，如果不存在
    os.makedirs(output_dir, exist_ok=True)

    for grib_file in grib_files:
        grib_file_path = os.path.join(grib_dir, grib_file)
        nc_file_name = grib_file.replace('.grib', '.nc')
        nc_file_path = os.path.join(output_dir, nc_file_name)

        # 转换每个 GRIB 文件
        convert_grib_to_nc(grib_file_path, nc_file_path)

# 使用示例
grib_directory = r'E:\PythonProjiects\Data_of_energy_competition'  # 输入 GRIB 文件夹路径
output_directory = r'E:\PythonProjiects\Data_of_energy_competition\NetCDF'  # 输出 NetCDF 文件夹路径

batch_convert_gribs(grib_directory, output_directory)
