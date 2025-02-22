from folium.plugins import MarkerCluster
import datalibs as dpl
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
import numpy as np

# 加载数据，只读取经纬度和容量列
data = pd.read_excel(dpl.WIND_POWER_BANK_PATH, usecols=['Latitude', 'Longitude', 'Capacity (MW)'])

# 按容量排序并选择前20%的数据
top_percentage = 0.2  # 选择前20%
data = data.sort_values('Capacity (MW)', ascending=False)
data = data.head(int(len(data) * top_percentage))

# 创建地图对象
initial_location = [data['Latitude'].iloc[0], data['Longitude'].iloc[0]]
m = folium.Map(
    location=initial_location,
    zoom_start=2,
    tiles='OpenStreetMap',
    attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
)

# 定义容量区间和对应颜色
capacity_bins = [0, 100, 500, 1000, 5000, float('inf')]  # 6 个边界值
colors = ['green', 'blue', 'orange', 'red', 'purple']  # 5 个颜色值

# 添加发电厂标记
for idx, row in data.iterrows():
    # 根据容量大小确定颜色
    capacity = row['Capacity (MW)']
    for i, bin_edge in enumerate(capacity_bins):
        if capacity <= bin_edge:
            color = colors[i - 1]  # 使用 i-1 来匹配颜色
            break

    # 创建弹出窗口内容
    popup_content = f"""
    <b>Capacity:</b> {row['Capacity (MW)']} MW<br>
    <b>Latitude:</b> {row['Latitude']}<br>
    <b>Longitude:</b> {row['Longitude']}
    """

    # 添加散点标记
    folium.CircleMarker(
        location=[row['Latitude'], row['Longitude']],
        radius=3,  # 固定大小的小圆点
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.8,
        popup=folium.Popup(popup_content, max_width=300)
    ).add_to(m)

# 保存为 HTML 文件
m.save("power_plants_scatter_map.html")