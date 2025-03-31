from .workflow_processor import WorkFlowProcessor
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, ttk, messagebox, scrolledtext
import threading
import re
import logging
from .logger_config import add_ui_handler, remove_ui_handler, set_ui_handler_level

class KeywordClassifierGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("关键词分类器")
        self.root.geometry("800x600")
        self.root.minsize(800, 600)
        
        self.rules_path = None
        self.keywords_path = None
        self.case_sensitive = tk.BooleanVar(value=False)
        self.separator = tk.StringVar(value="&")
        
        self.create_widgets()
        
        # 用于存储处理器实例
        self.processor = None
        
        # 用于存储日志信息
        self.log_queue = []
        
        # 设置UI日志处理器
        self.ui_handler = add_ui_handler(self.log_callback, level=logging.INFO)
        
    def create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 文件选择区域
        file_frame = ttk.LabelFrame(main_frame, text="文件选择", padding="10")
        file_frame.pack(fill=tk.X, pady=5)
        
        # 规则文件选择
        ttk.Label(file_frame, text="规则文件:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.rules_path_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.rules_path_var, width=50).grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(file_frame, text="浏览...", command=self.browse_rules_file).grid(row=0, column=2, padx=5, pady=5)
        
        # 关键词文件选择
        ttk.Label(file_frame, text="关键词文件:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.keywords_path_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.keywords_path_var, width=50).grid(row=1, column=1, padx=5, pady=5)
        ttk.Button(file_frame, text="浏览...", command=self.browse_keywords_file).grid(row=1, column=2, padx=5, pady=5)
        
        # 设置区域
        settings_frame = ttk.LabelFrame(main_frame, text="设置", padding="10")
        settings_frame.pack(fill=tk.X, pady=5)
        
        # 大小写敏感选项
        ttk.Checkbutton(settings_frame, text="大小写敏感", variable=self.case_sensitive).grid(row=0, column=0, sticky=tk.W, pady=5)
        
        # 分隔符设置
        ttk.Label(settings_frame, text="分隔符:").grid(row=0, column=1, sticky=tk.W, padx=20, pady=5)
        separator_entry = ttk.Entry(settings_frame, textvariable=self.separator, width=5)
        separator_entry.grid(row=0, column=2, sticky=tk.W, pady=5)
        
        # 日志级别设置
        ttk.Label(settings_frame, text="日志级别:").grid(row=0, column=3, sticky=tk.W, padx=20, pady=5)
        self.log_level = tk.StringVar(value="INFO")
        log_level_combo = ttk.Combobox(settings_frame, textvariable=self.log_level, width=10, state="readonly")
        log_level_combo["values"] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        log_level_combo.grid(row=0, column=4, sticky=tk.W, pady=5)
        log_level_combo.bind("<<ComboboxSelected>>", self.on_log_level_change)
        
        # 分隔符提示
        ttk.Label(settings_frame, text="注意: 分隔符不可以是有实际分词功能的符号 (如空格、逗号等)").grid(row=1, column=0, columnspan=5, sticky=tk.W, pady=5)
        
        # 操作按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(button_frame, text="开始处理", command=self.start_processing).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="清空日志", command=self.clear_log).pack(side=tk.LEFT, padx=5)
        
        # 日志区域
        log_frame = ttk.LabelFrame(main_frame, text="处理日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, width=80, height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
    
    def browse_rules_file(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel 文件", "*.xlsx"), ("所有文件", "*.*")])
        if filename:
            self.rules_path_var.set(filename)
    
    def browse_keywords_file(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel 文件", "*.xlsx"), ("所有文件", "*.*")])
        if filename:
            self.keywords_path_var.set(filename)
    
    def validate_separator(self, separator):
        # 检查分隔符是否为分词功能符号
        invalid_separators = [' ', ',', '.', '\t', '\n', '\r']
        if separator in invalid_separators:
            return False, f"分隔符不能是 '{separator}', 它具有分词功能"
        
        # 检查是否为特殊字符
        if re.match(r'[\[\]()<>|+]', separator):
            return False, f"分隔符不能是 '{separator}', 它是规则解析中的特殊字符"
        
        return True, ""
    
    def log_message(self, message):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def error_callback(self, error_message):
        # 这个回调函数会被传递给WorkFlowProcessor
        self.log_queue.append(("ERROR", error_message))
        
    def log_callback(self, level, message):
        """处理来自日志系统的消息
        
        Args:
            level: 日志级别
            message: 日志消息
        """
        self.log_queue.append((level, message))
    
    def on_log_level_change(self, event=None):
        """当日志级别变更时调用"""
        level_str = self.log_level.get()
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        level = level_map.get(level_str, logging.INFO)
        set_ui_handler_level(level)
        self.log_message(f"日志级别已设置为: {level_str}")
    
    def update_log_from_queue(self):
        if self.log_queue:
            self.log_text.config(state=tk.NORMAL)
            for log_type, message in self.log_queue:
                if log_type == "ERROR" or log_type == "CRITICAL":
                    self.log_text.insert(tk.END, f"{message}\n", "error")
                elif log_type == "WARNING":
                    self.log_text.insert(tk.END, f"{message}\n", "warning")
                elif log_type == "DEBUG":
                    self.log_text.insert(tk.END, f"{message}\n", "debug")
                else:  # INFO and others
                    self.log_text.insert(tk.END, f"{message}\n", "info")
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
            self.log_queue.clear()
        
        # 继续更新日志
        self.root.after(100, self.update_log_from_queue)
    
    def clear_log(self):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def start_processing(self):
        # 获取用户输入
        rules_path_str = self.rules_path_var.get()
        keywords_path_str = self.keywords_path_var.get()
        case_sensitive = self.case_sensitive.get()
        separator = self.separator.get()
        
        # 验证输入
        if not rules_path_str:
            messagebox.showerror("错误", "请选择规则文件")
            return
        
        if not keywords_path_str:
            messagebox.showerror("错误", "请选择关键词文件")
            return
        
        # 验证分隔符
        is_valid, error_message = self.validate_separator(separator)
        if not is_valid:
            messagebox.showerror("错误", error_message)
            return
        
        # 转换为Path对象
        rules_path = Path(rules_path_str)
        keywords_path = Path(keywords_path_str)
        
        # 检查文件是否存在
        if not rules_path.exists():
            messagebox.showerror("错误", f"规则文件不存在: {rules_path}")
            return
        
        if not keywords_path.exists():
            messagebox.showerror("错误", f"关键词文件不存在: {keywords_path}")
            return
        
        # 清空日志
        self.clear_log()
        
        # 设置日志样式
        self.log_text.tag_configure("error", foreground="red")
        self.log_text.tag_configure("warning", foreground="orange")
        self.log_text.tag_configure("info", foreground="black")
        self.log_text.tag_configure("debug", foreground="gray")
        
        # 开始处理
        self.log_message("开始处理...")
        self.log_message(f"规则文件: {rules_path}")
        self.log_message(f"关键词文件: {keywords_path}")
        self.log_message(f"大小写敏感: {'是' if case_sensitive else '否'}")
        self.log_message(f"分隔符: {separator}")
        
        # 在新线程中运行处理过程
        threading.Thread(target=self.process_workflow, args=(rules_path, keywords_path, case_sensitive, separator)).start()
        
        # 开始更新日志
        self.update_log_from_queue()
    
    def process_workflow(self, rules_path, keywords_path, case_sensitive, separator):
        try:
            # 创建处理器实例
            self.processor = WorkFlowProcessor(error_callback=self.error_callback)
            
            # 设置关键词分类器的参数
            self.processor.classifier.case_sensitive = case_sensitive
            self.processor.classifier.separator = separator
            
            # 记录处理开始
            self.log_queue.append(("INFO", "正在处理工作流...这可能需要一些时间，请耐心等待"))
            
            # 处理工作流
            result = self.processor.process_workflow(rules_path, keywords_path)
            
            # 记录处理完成
            if result:
                self.log_queue.append(("INFO", "处理完成！"))
                self.log_queue.append(("INFO", f"结果已保存到: {self.processor.output_dir}"))
                
                # 在主线程中显示成功消息
                self.root.after(0, lambda: messagebox.showinfo("成功", f"处理完成！结果已保存到: {self.processor.output_dir}"))
            else:
                self.log_queue.append(("ERROR", "处理失败，请查看错误信息"))
        except Exception as e:
            msg_err = f"处理过程中发生错误: {str(e)}"
            self.log_queue.append(("ERROR", msg_err))
            # 在主线程中显示错误消息
            self.root.after(0, lambda: messagebox.showerror("错误", msg_err))

def main():
    root = tk.Tk()
    app = KeywordClassifierGUI(root)
    root.mainloop()

if __name__ == '__main__':
    main()
