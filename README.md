## 项目目录：
```plaintext
Data_of_energy_competition
├── powerbank_location_cluster_distribution 电场分布处理
│   ├── draw_power_bank_cluster.py   按聚落画风电场的分布图
│   ├── draw_power_bank_distribution.py 
│   ├── power_plants_interactive_map.html
│   └── power_plants_interactive_map_cluster.html
├── preprocessing ERA5-LAND 风速、表面气压数据预处理
│   ├── combineTwoGrib.py 合并两个grib文件
│   ├── merge_days_to_month.py  把按天下载的文件合并为一个月
│   ├── process_log.txt 以上 merge_days_to_month.py 合并产生的日志
│   ├── showgrib.py 输出文件含有的消息数量和基本信息
│   ├── showMessages.py 暂时未成功，想要快速查看
│   └── sort.py 把合并后的文件中的消息按时间顺序排序，防止因为合并产生时间错误
├── datalibs.py 本地数据目录的索引
├── README.md
└── try.py 修改后缀名
```
