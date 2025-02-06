import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import eccodes
import csv
from datetime import datetime
import os


class GRIBViewerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("GRIB 文件分析器")
        self.root.geometry("1200x600")

        # 初始化数据结构
        self.all_messages = []

        # 创建界面组件
        self.create_widgets()

    def create_widgets(self):
        # 顶部按钮框架
        button_frame = ttk.Frame(self.root)
        button_frame.pack(pady=10, fill=tk.X)

        # 文件选择按钮
        self.select_btn = ttk.Button(
            button_frame,
            text="选择 GRIB 文件",
            command=self.load_grib_files
        )
        self.select_btn.pack(side=tk.LEFT, padx=5)

        # 保存按钮
        self.save_btn = ttk.Button(
            button_frame,
            text="保存为CSV",
            command=self.export_to_csv
        )
        self.save_btn.pack(side=tk.LEFT, padx=5)

        # 创建表格
        self.create_table()

    def create_table(self):
        # 表格框架
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 滚动条
        scroll_y = ttk.Scrollbar(table_frame)
        scroll_x = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)

        # 表格列定义
        columns = [
            ("filename", "文件名", 150),
            ("message_no", "消息编号", 80),
            ("centre", "数据来源", 120),
            ("obs_time", "观测时间", 150),
            ("forecast", "预报时长", 100),
            ("param_id", "参数ID", 80),
            ("grid", "网格类型", 120),
            ("min_val", "最小值", 80),
            ("max_val", "最大值", 80),
            ("values", "数据量", 80)
        ]

        # 创建表格
        self.table = ttk.Treeview(
            table_frame,
            columns=[col[0] for col in columns],
            yscrollcommand=scroll_y.set,
            xscrollcommand=scroll_x.set,
            selectmode="extended",
            height=20
        )

        # 配置列
        for col in columns:
            self.table.heading(col[0], text=col[1], anchor=tk.W)
            self.table.column(col[0], width=col[2], minwidth=col[2])

        # 布局组件
        self.table.grid(row=0, column=0, sticky="nsew")
        scroll_y.config(command=self.table.yview)
        scroll_x.config(command=self.table.xview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        # 配置网格布局
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

    def load_grib_files(self):
        """加载并解析GRIB文件"""
        filepaths = filedialog.askopenfilenames(
            title="选择GRIB文件",
            filetypes=[("GRIB files", "*.grib *.grb *.grib_cache"), ("All files", "*.*")]
        )

        if not filepaths:
            return

        # 清空旧数据
        self.all_messages.clear()
        self.table.delete(*self.table.get_children())

        # 解析每个文件
        for filepath in filepaths:
            try:
                messages = self.parse_grib_file(filepath)
                self.all_messages.extend(messages)
            except Exception as e:
                messagebox.showerror("错误", f"解析文件失败: {os.path.basename(filepath)}\n{str(e)}")

        # 更新表格
        for msg in self.all_messages:
            self.table.insert("", tk.END, values=(
                msg["filename"],
                msg["message_no"],
                msg["centre"],
                f"{msg['data_date']} {msg['data_time']}",
                f"{msg['start_step']}小时",
                msg["param_id"],
                msg["grid_type"],
                f"{msg['min']:.2f}",
                f"{msg['max']:.2f}",
                msg["values_count"]
            ))

    def parse_grib_file(self, filepath):
        """解析单个GRIB文件"""
        messages = []
        filename = os.path.basename(filepath)

        with open(filepath, "rb") as f:
            while True:
                msg = eccodes.codes_grib_new_from_file(f)
                if not msg:
                    break

                try:
                    data = {
                        "filename": filename,
                        "message_no": self._get_msg_value(msg, "messageNumber"),
                        "centre": self._get_msg_value(msg, "centreDescription"),
                        "data_date": self._get_msg_value(msg, "dataDate"),
                        "data_time": f"{self._get_msg_value(msg, 'dataTime'):04d}",
                        "start_step": self._get_msg_value(msg, "startStep"),
                        "grid_type": self._get_msg_value(msg, "gridType"),
                        "param_id": self._get_msg_value(msg, "paramId"),
                        "min": self._get_msg_value(msg, "min", 0.0),
                        "max": self._get_msg_value(msg, "max", 0.0),
                        "values_count": self._get_msg_value(msg, "numberOfValues", 0)
                    }
                    messages.append(data)
                finally:
                    eccodes.codes_release(msg)

        return messages

    def _get_msg_value(self, msg, key, default="N/A"):
        """安全获取消息值"""
        try:
            if eccodes.codes_is_defined(msg, key):
                return eccodes.codes_get(msg, key)
            return default
        except Exception as e:
            print(f"获取{key}失败: {str(e)}")
            return default

    def export_to_csv(self):
        """导出为CSV文件"""
        if not self.all_messages:
            messagebox.showwarning("警告", "没有可导出的数据")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        default_filename = f"grib_export_{timestamp}.csv"

        save_path = filedialog.asksaveasfilename(
            title="保存CSV文件",
            initialfile=default_filename,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")]
        )

        if not save_path:
            return

        try:
            with open(save_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # 写入表头
                writer.writerow([
                    "文件名", "消息编号", "数据来源", "观测日期", "观测时间",
                    "预报时长(小时)", "参数ID", "网格类型", "最小值", "最大值", "数据量"
                ])
                # 写入数据
                for msg in self.all_messages:
                    writer.writerow([
                        msg["filename"],
                        msg["message_no"],
                        msg["centre"],
                        msg["data_date"],
                        msg["data_time"],
                        msg["start_step"],
                        msg["param_id"],
                        msg["grid_type"],
                        msg["min"],
                        msg["max"],
                        msg["values_count"]
                    ])
            messagebox.showinfo("成功", f"文件已保存至: {save_path}")
        except Exception as e:
            messagebox.showerror("保存失败", f"保存CSV时出错: {str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = GRIBViewerApp(root)
    root.mainloop()