import pandas as pd
import folium
import numpy as np
from folium.plugins import MarkerCluster

import datalibs as dpl

# 加载数据
data = pd.read_csv(dpl.WIND_POWER_BANK_PATH)

# 创建地图对象
initial_location = [data['latitude'].iloc[0], data['longitude'].iloc[0]]
m = folium.Map(location=initial_location, zoom_start=2)


# 归一化半径函数（对数缩放）
def normalize_radius(capacity):
    # 使用对数缩放，并限制半径范围
    return np.log10(capacity + 1) * 3  # 调整缩放系数以控制圆圈大小


# 添加发电厂标记
for idx, row in data.iterrows():
    # 归一化半径
    radius = normalize_radius(row['capacity_mw'])

    # 创建弹出窗口内容
    popup_content = f"""
    <b>Country:</b> {row['country']}<br>
    <b>Capacity:</b> {row['capacity_mw']} MW<br>
    <b>Latitude:</b> {row['latitude']}<br>
    <b>Longitude:</b> {row['longitude']}
    """

    # 添加圆圈标记
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius,  # 使用归一化后的半径
        color='blue',  # 圆圈边框颜色
        fill=True,  # 填充圆圈
        fill_color='blue',  # 圆圈填充颜色
        fill_opacity=0.6,  # 填充透明度
        popup=folium.Popup(popup_content, max_width=300)  # 弹出窗口
    ).add_to(m)

# 保存为 HTML 文件
m.save("power_plants_interactive_map.html")
