import tkinter as tk
from tkinter import ttk
import time
from typing import Optional, Union
from .tk_root import root

class TkProgressBar:
    def __init__(self, 
                 iterable=None, 
                 desc: str = None, 
                 total: int = None, 
                 parent: Optional[tk.Tk] = root,
                 leave: bool = True,
                 **kwargs):
        """
        自定义 Tkinter 进度条，兼容 tqdm 基本 API
        
        参数:
            iterable: 可迭代对象
            desc: 进度条描述
            total: 总进度数
            parent: 父 Tkinter 窗口，如果为 None 则创建新窗口
            leave: 完成后是否保留进度条
            **kwargs: 其他 tqdm 兼容参数（部分支持）
        """
        self.iterable = iterable
        self.desc = desc or ""
        self.total = total or (len(iterable) if iterable else 100)
        self.leave = leave
        self.parent = parent
        self.parent_window = None
        self.n = 0
        self.last_update = time.time()
        # 初始化进度条
        self._setup_ui()

        self.last_update = time.time()
        
        # 如果传入了可迭代对象，包装为迭代器
        if iterable is not None:
            self.iter = iter(iterable)
        else:
            self.iter = None
    
    def _setup_ui(self):
        """初始化 UI 组件"""
        # 如果没有提供父窗口，则创建新窗口
        if self.parent is None:
            # 检查是否已有Tk实例
            if not tk._default_root:
                self.parent_window = tk.Tk()
                self.parent_window.title("进度")
            else:
                self.parent_window = tk.Toplevel()
                self.parent_window.title("进度")
            self.parent = self.parent_window
        
        self.frame = ttk.Frame(self.parent)
        self.frame.pack(padx=10, pady=10)
        
        # 描述标签
        if self.desc:
            self.desc_label = ttk.Label(self.frame, text=self.desc)
            self.desc_label.pack(anchor="w")
        
        # 进度条
        self.progress = ttk.Progressbar(
            self.frame, 
            orient="horizontal", 
            length=300,
            mode="determinate",
            maximum=self.total
        )
        self.progress.pack(fill="x", expand=True)
        
        # 信息标签（百分比、速度等）
        self.info_label = ttk.Label(self.frame)
        self.info_label.pack()
        
        # 立即更新 UI
        self._update_ui()
    
    def _update_ui(self):
        """更新进度条 UI"""
        percent = min(100, (self.n / self.total) * 100)
        self.progress["value"] = self.n
        
        # 计算速度
        now = time.time()
        elapsed = now - self.last_update if self.n > 0 else 0
        speed = self.n / elapsed if elapsed > 0 else 0
        
        info_text = f"{percent:.1f}%"
        if speed > 0:
            info_text += f" | {speed:.1f} it/s"
        if self.total and self.total > 0:
            info_text += f" | {self.n}/{self.total}"
        
        self.info_label.config(text=info_text)
        self.parent.update()
        self.last_update = now
    
    def update(self, n=1):
        """更新进度"""
        self.n += n
        self._update_ui()
    
    def close(self):
        """关闭进度条"""
        if not self.leave and self.parent_window:
            self.parent_window.destroy()
    
    def __iter__(self):
        """使实例可迭代"""
        return self
    
    def __next__(self):
        """迭代接口"""
        if self.iter is None:
            raise StopIteration
        
        try:
            result = next(self.iter)
            self.update(1)
            return result
        except StopIteration:
            self.close()
            raise
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
    
    def set_description(self, desc: str):
        """设置描述文本"""
        self.desc = desc
        if hasattr(self, 'desc_label'):
            self.desc_label.config(text=desc)
        self._update_ui()

# 使用示例
if __name__ == "__main__":
    # 示例1: 独立窗口
    with TkProgressBar(range(100), desc="处理中...") as pbar:
        for i in pbar:
            time.sleep(0.05)  # 模拟工作
    
    # 示例2: 嵌入现有窗口
    root = tk.Tk()
    root.title("主窗口")
    
    def start_task():
        items = [f"Item {i}" for i in range(50)]
        with TkProgressBar(items, desc="处理数据", parent=root) as pbar:
            for item in pbar:
                time.sleep(0.1)  # 模拟处理每个项目
    
    btn = ttk.Button(root, text="开始任务", command=start_task)
    btn.pack(pady=20)
    
    root.mainloop()