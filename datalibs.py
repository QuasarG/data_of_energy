# this is a file containing all the data paths

# 电厂数据路径
WIND_POWER_BANK_PATH = 'E:\\大二上\\竞赛\\能经大赛\\data\\globalpowerplantdatabasev130\\global_wind_power_plant_database.csv'

# 每日平均气温
AVERAGE_DAILY_TEMPRATURE = {
    year: f'E:\\大二上\\竞赛\\能经大赛\\data\\excel格式的数据\\TEMP_{year}_daily.xlsx'
    for year in range(1929, 2024 + 1)  # 使用字典推导式
}

# 1990 - 2024年风速 grib 数据 路径
import calendar

WIND_SPEED_GRIB = {
    year: f'E:\\大二上\\竞赛\\能经大赛\\data\\grib格式的数据\\{year:04d}-{month:02d}-{day:02d}.grib'
    for year in range(1990, 2024 + 1)
    for month in range(1, 12 + 1)
    for day in range(1, calendar.monthrange(year, month)[1] + 1)
}
