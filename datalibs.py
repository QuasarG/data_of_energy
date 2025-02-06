# this is a file containing all the data paths

# 电厂数据路径
WIND_POWER_BANK_PATH = 'E:\\大二上\\竞赛\\能经大赛\\data\\globalpowerplantdatabasev130\\global_wind_power_plant_database.csv'

# 每日平均气温
AVERAGE_DAILY_TEMPRATURE = {
    year: f'E:\\大二上\\竞赛\\能经大赛\\data\\excel格式的数据\\TEMP_{year}_daily.xlsx'
    for year in range(1929, 2024 + 1)  # 使用字典推导式
}

# 1990 - 2024年风速 grib_cache 数据 路径
# 存在3个目录下
dir1 = r'M:\era5'
dir2 = r'F:\data_from_era5'
dir3 = r'G:\era5'
