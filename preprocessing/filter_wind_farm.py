import pandas as pd

def filter_wind_farm(input_file, output_file):
    """筛选风电场数据，只保留在建或运营中的陆上风电场
    
    Args:
        input_file (str): 输入Excel文件路径
        output_file (str): 输出Excel文件路径
    """
    try:
        # 读取Excel文件
        print(f'正在读取文件: {input_file}')
        df = pd.read_excel(input_file)
        original_count = len(df)
        print(f'原始数据行数: {original_count}')
        
        # 筛选条件：Status为operating或construction，且Installation Type为Onshore
        status_mask = df['Status'].isin(['operating', 'construction'])
        type_mask = df['Installation Type'] == 'Onshore'
        
        # 应用筛选条件
        filtered_df = df[status_mask & type_mask]
        filtered_count = len(filtered_df)
        
        # 保存结果
        filtered_df.to_excel(output_file, index=False)
        
        # 输出统计信息
        print(f'\n处理完成:')
        print(f'筛选后数据行数: {filtered_count}')
        print(f'删除的行数: {original_count - filtered_count}')
        print(f'保留比例: {(filtered_count/original_count)*100:.2f}%')
        print(f'\n结果已保存至: {output_file}')
        
    except Exception as e:
        print(f'处理过程中出错: {str(e)}')

def main():
    # 设置输入输出文件路径
    input_file = r'E:\\大二上\\竞赛\\能经大赛\\data\\global_wind_farm.xlsx'
    output_file = 'filtered_wind_farm.xlsx'
    
    # 执行筛选
    filter_wind_farm(input_file, output_file)

if __name__ == '__main__':
    main()