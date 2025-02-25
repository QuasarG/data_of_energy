import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import folium
import numpy as np
from folium.plugins import HeatMap
import contextily as ctx


def plot_static_map(data):
    """使用matplotlib绘制静态地图版本"""
    # 创建世界地图底图
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    # 创建图形
    fig, ax = plt.subplots(figsize=(15, 10))

    # 绘制世界地图底图
    world.plot(ax=ax, alpha=0.5, color='lightgray')

    # 设置容量区间和颜色
    capacity_bins = [0, 100, 500, 1000, 5000, float('inf')]
    colors = ['green', 'blue', 'orange', 'red', 'purple']

    # 根据容量分组绘制散点
    for i in range(len(capacity_bins) - 1):
        mask = (data['Capacity (MW)'] > capacity_bins[i]) & (data['Capacity (MW)'] <= capacity_bins[i + 1])
        subset = data[mask]
        ax.scatter(subset['Longitude'], subset['Latitude'],
                   c=colors[i], alpha=0.6, s=30,
                   label=f'{capacity_bins[i]}-{capacity_bins[i + 1]} MW')

    # 设置地图范围和标题
    ax.set_xlim([-180, 180])
    ax.set_ylim([-90, 90])
    ax.set_title('Global Wind Power Plants Distribution')
    ax.legend()

    return fig


def plot_interactive_map(data):
    """使用folium绘制交互式地图版本"""
    # 创建地图对象
    m = folium.Map(location=[30, 0], zoom_start=2, control_scale=True)

    # 添加散点图层
    for idx, row in data.iterrows():
        capacity = row['Capacity (MW)']

        # 根据容量确定圆点大小和颜色
        if capacity <= 100:
            color = '#00ff00'
            radius = 3
        elif capacity <= 500:
            color = '#0000ff'
            radius = 5
        elif capacity <= 1000:
            color = '#ffa500'
            radius = 7
        elif capacity <= 5000:
            color = '#ff0000'
            radius = 9
        else:
            color = '#800080'
            radius = 11

        # 使用Circle代替Marker
        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=radius,  # 圆点大小
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=1,
            popup=f"Capacity: {capacity} MW"
        ).add_to(m)

    return m


# 主程序
if __name__ == "__main__":
    # 加载数据
    data = pd.read_excel(r'E:\PythonProjiects\Data_of_energy_competition\filtered_wind_farm.xlsx', usecols=['Latitude', 'Longitude', 'Capacity (MW)'])

    # 绘制静态地图
    fig = plot_static_map(data)
    plt.savefig('wind_power_static_map.png', dpi=300, bbox_inches='tight')

    # 绘制交互式地图
    m = plot_interactive_map(data)
    m.save('wind_power_interactive_map.html')