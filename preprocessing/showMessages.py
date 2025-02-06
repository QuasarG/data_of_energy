import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import subprocess
import csv
from datetime import datetime
import os
import platform


class GRIBViewerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("GRIB 文件分析器 (优化版)")
        self.root.geometry("1400x800")

        # 初始化数据结构
        self.all_messages = []

        # 配置grib_ls路径
        self.grib_ls_path = self.find_grib_ls()
        if not self.grib_ls_path:
            messagebox.showerror("错误", "未找到grib_ls命令，请确保已正确安装ECCODES")
            root.destroy()
            return

        # 创建界面组件
        self.create_widgets()

    def find_grib_ls(self):
        """在不同平台上查找grib_ls路径"""
        try:
            if platform.system() == 'Windows':
                result = subprocess.run(['where', 'grib_ls'], capture_output=True, text=True)
            else:
                result = subprocess.run(['which', 'grib_ls'], capture_output=True, text=True)

            paths = result.stdout.strip().split('\n')
            return paths[0] if paths else None
        except:
            return None

    def create_widgets(self):
        # 顶部控制面板
        control_frame = ttk.Frame(self.root)
        control_frame.pack(pady=10, fill=tk.X)

        # 文件选择按钮
        self.select_btn = ttk.Button(
            control_frame,
            text="选择 GRIB 文件",
            command=self.load_grib_files
        )
        self.select_btn.pack(side=tk.LEFT, padx=5)

        # 进度条
        self.progress = ttk.Progressbar(
            control_frame,
            orient=tk.HORIZONTAL,
            mode='determinate'
        )
        self.progress.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # 状态标签
        self.status = ttk.Label(control_frame, text="就绪")
        self.status.pack(side=tk.LEFT, padx=5)

        # 创建表格
        self.create_table()

    def create_table(self):
        """创建带滚动条的表格"""
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 列配置
        columns = [
            ('file', '文件名', 180),
            ('param', '参数', 120),
            ('level', '高度层', 80),
            ('step', '预报步长', 90),
            ('date', '日期', 120),
            ('time', '时间', 80),
            ('min', '最小值', 90),
            ('max', '最大值', 90),
            ('count', '数据点数', 90),
            ('grid', '网格类型', 120)
        ]

        # 创建Treeview
        self.table = ttk.Treeview(
            table_frame,
            columns=[c[0] for c in columns],
            show='headings',
            selectmode='extended'
        )

        # 配置列标题和宽度
        for col in columns:
            self.table.heading(col[0], text=col[1])
            self.table.column(col[0], width=col[2], anchor=tk.CENTER)

        # 滚动条
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.table.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.table.xview)
        self.table.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # 布局
        self.table.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

    def load_grib_files(self):
        """使用grib_ls加载文件"""
        filepaths = filedialog.askopenfilenames(
            title="选择GRIB文件",
            filetypes=[("GRIB files", "*.grib *.grb *.grib2 *.gb2 *.grib_cache"), ("All files", "*.*")]
        )

        if not filepaths:
            return

        # 重置状态
        self.all_messages = []
        self.table.delete(*self.table.get_children())
        total_files = len(filepaths)

        try:
            for idx, filepath in enumerate(filepaths, 1):
                self.status.config(text=f"正在处理: {os.path.basename(filepath)} ({idx}/{total_files})")
                self.progress['value'] = (idx / total_files) * 100
                self.root.update_idletasks()

                messages = self.parse_with_grib_ls(filepath)
                self.all_messages.extend(messages)

                # 增量更新表格
                for msg in messages:
                    self.table.insert('', 'end', values=(
                        msg['file'],
                        f"{msg['shortName']} ({msg['paramId']})",
                        msg.get('level', 'N/A'),
                        f"{msg['step']}小时",
                        msg['dataDate'],
                        msg['dataTime'],
                        f"{msg['min']:.2f}",
                        f"{msg['max']:.2f}",
                        msg['count'],
                        msg['gridType']
                    ))

            self.status.config(text=f"完成！共处理 {len(self.all_messages)} 条消息")
            self.progress['value'] = 0
        except Exception as e:
            messagebox.showerror("处理错误", str(e))
            self.status.config(text="处理出错")
            self.progress['value'] = 0

    def parse_with_grib_ls(self, filepath):
        """使用grib_ls解析文件"""
        cmd = [
            self.grib_ls_path,
            '-p', 'dataDate,dataTime,step,paramId,shortName,level,min,max,numberOfValues,gridType',
            '-w', 'count=1',  # 只获取第一个有效消息
            '-n', 'ls',  # 列表模式
            filepath
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=300  # 5分钟超时
            )
            return self.parse_grib_ls_output(result.stdout, filepath)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"处理超时: {os.path.basename(filepath)}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"grib_ls错误: {e.stderr}")

    def parse_grib_ls_output(self, output, filepath):
        """解析grib_ls输出"""
        messages = []
        current_msg = {}
        filename = os.path.basename(filepath)

        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue

            # 处理键值对
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # 处理特殊值
                if key in ['paramId', 'step', 'numberOfValues']:
                    value = int(value)
                elif key in ['min', 'max']:
                    value = float(value)

                current_msg[key] = value
            elif line.startswith('---'):
                # 完成一个消息的解析
                if current_msg:
                    current_msg.update({
                        'file': filename,
                        'count': current_msg.get('numberOfValues', 0)
                    })
                    messages.append(current_msg)
                    current_msg = {}

        return messages

    def export_to_csv(self):
        """导出为CSV"""
        if not self.all_messages:
            messagebox.showwarning("无数据", "请先加载数据")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"grib_export_{timestamp}.csv"

        save_path = filedialog.asksaveasfilename(
            title="保存CSV文件",
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv")]
        )

        if not save_path:
            return

        try:
            with open(save_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # 写表头
                writer.writerow([
                    '文件名', '参数ID', '参数简称', '日期', '时间',
                    '预报步长', '高度层', '最小值', '最大值',
                    '数据点数', '网格类型'
                ])
                # 写数据
                for msg in self.all_messages:
                    writer.writerow([
                        msg['file'],
                        msg['paramId'],
                        msg.get('shortName', 'N/A'),
                        msg['dataDate'],
                        msg['dataTime'],
                        msg['step'],
                        msg.get('level', 'N/A'),
                        msg['min'],
                        msg['max'],
                        msg['count'],
                        msg['gridType']
                    ])
            messagebox.showinfo("成功", f"文件已保存至:\n{save_path}")
        except Exception as e:
            messagebox.showerror("保存失败", str(e))


if __name__ == "__main__":
    root = tk.Tk()
    app = GRIBViewerApp(root)
    root.mainloop()