import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import toml
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from .message import message
from .core import KeywordClassifier
from .excel_handler import read_keywords, read_work_flow_rules, save_classified_keywords
from .models import FileInfo, ClassifiedKeywordDTO
from .utils import trans_classified_keyword_to_next_source_keyword,get_exe_dir,get_func_env
from .tk_root import root

class KeywordClassifierGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("关键词分类工具")
        self.root.geometry("800x600")
        

        # Config file path
        self.config_path = get_exe_dir() / "config.toml"
        
        # Variables
        self.rule_file = tk.StringVar()
        self.keyword_file = tk.StringVar()
        # Default to '工作流结果' subfolder, create if needed
        func_env = get_func_env()
        if func_env == 'py':
            output_dir = get_exe_dir().parent.parent / "工作流结果"
        else:
            output_dir =get_exe_dir() / "工作流结果"
        output_dir.mkdir(exist_ok=True)
        self.output_dir = tk.StringVar(value=str(output_dir))
        self.case_sensitive = tk.BooleanVar()
        self.load_config()
        
        # Create UI
        self.create_widgets()
        
        message.set_handler(self.update_message)
    def load_config(self):
        """Load configuration from toml file"""
        try:
            config = toml.load(self.config_path)
            self.case_sensitive.set(config.get("settings", {}).get("case_sensitive", False))
        except (FileNotFoundError, toml.TomlDecodeError):
            self.case_sensitive.set(False)
    
    def save_config(self):
        """Save configuration to toml file"""
        config = {"settings": {"case_sensitive": self.case_sensitive.get()}}
        with open(self.config_path, "w") as f:
            toml.dump(config, f)
    
    def create_widgets(self):
        """Create all GUI widgets"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Input files frame
        input_frame = ttk.LabelFrame(main_frame, text="输入文件", padding="10")
        input_frame.pack(fill=tk.X, pady=5)
        
        # Rule file
        ttk.Label(input_frame, text="规则文件:").grid(row=0, column=0, sticky=tk.W)
        rule_entry = ttk.Entry(input_frame, textvariable=self.rule_file, width=50)
        rule_entry.grid(row=0, column=1, padx=5)
        ttk.Button(input_frame, text="浏览...", command=self.browse_rule_file).grid(row=0, column=2)
        
        # Keyword file
        ttk.Label(input_frame, text="关键词文件:").grid(row=1, column=0, sticky=tk.W)
        keyword_entry = ttk.Entry(input_frame, textvariable=self.keyword_file, width=50)
        keyword_entry.grid(row=1, column=1, padx=5)
        ttk.Button(input_frame, text="浏览...", command=self.browse_keyword_file).grid(row=1, column=2)
        
        # Options frame
        options_frame = ttk.LabelFrame(main_frame, text="选项", padding="10")
        options_frame.pack(fill=tk.X, pady=5)
        
        # Case sensitive
        ttk.Checkbutton(
            options_frame, 
            text="大小写敏感", 
            variable=self.case_sensitive,
            command=self.save_config
        ).pack(anchor=tk.W)
        
        # Output directory
        ttk.Label(options_frame, text="输出目录:").pack(anchor=tk.W)
        output_frame = ttk.Frame(options_frame)
        output_frame.pack(fill=tk.X, pady=5)
        output_entry = ttk.Entry(output_frame, textvariable=self.output_dir, width=50)
        output_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(output_frame, text="浏览...", command=self.browse_output_dir).pack(side=tk.LEFT)
        
        # # Progress bar
        # self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, mode='determinate')
        # self.progress.pack(fill=tk.X, pady=10)
        
        # # Message display
        # msg_frame = ttk.LabelFrame(main_frame, text="消息", padding="10")
        # msg_frame.pack(fill=tk.BOTH, expand=True)
        
        # self.msg_text = tk.Text(msg_frame, wrap=tk.WORD, state=tk.DISABLED)
        # self.msg_text.pack(fill=tk.BOTH, expand=True)
        
        # # Bottom frame for the start button
        # bottom_frame = ttk.Frame(main_frame)
        # bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=10)
        
        # # Large, prominent start button
        # run_btn = ttk.Button(
        #     bottom_frame,
        #     text="开始分类",
        #     command=self.run_classification,
        #     style="Large.TButton"
        # )
        # run_btn.pack(fill=tk.X, ipady=15, ipadx=30)
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.progress.pack(fill=tk.X, pady=10)
        
        # Bottom frame for the start button
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=10) # Packed AFTER msg_frame

        # Large, prominent start button
        run_btn = ttk.Button(
            bottom_frame,
            text="开始分类",
            command=self.run_classification,
            style="Large.TButton"
        )
        run_btn.pack(fill=tk.X, ipady=15, ipadx=30)

        # Message display
        msg_frame = ttk.LabelFrame(main_frame, text="消息", padding="10")
        msg_frame.pack(fill=tk.BOTH, expand=True) # Packed BEFORE bottom_frame

        self.msg_text = tk.Text(msg_frame, wrap=tk.WORD, state=tk.DISABLED)
        self.msg_text.pack(fill=tk.BOTH, expand=True)

        
        # Configure button style
        style = ttk.Style()
        style.configure("Large.TButton",
                      font=('Microsoft YaHei', 12, 'bold'),
                      foreground='black',
                      background='#4CAF50',
                      padding=10,
                      borderwidth=0)
        style.map("Large.TButton",
                 background=[('active', '#45a049')])
    
    def browse_rule_file(self):
        """Browse for rule file"""
        filepath = filedialog.askopenfilename(
            title="选择规则文件",
            filetypes=[("Excel文件", "*.xlsx"), ("所有文件", "*.*")]
        )
        if filepath:
            self.rule_file.set(filepath)
            self.update_message(f"已选择规则文件: {filepath}")
    
    def browse_keyword_file(self):
        """Browse for keyword file"""
        filepath = filedialog.askopenfilename(
            title="选择关键词文件",
            filetypes=[("Excel文件", "*.xlsx"), ("所有文件", "*.*")]
        )
        if filepath:
            self.keyword_file.set(filepath)
            self.update_message(f"已选择关键词文件: {filepath}")
    
    def browse_output_dir(self):
        """Browse for output directory"""
        dirpath = filedialog.askdirectory(
            title="选择输出目录",
            initialdir=self.output_dir.get()
        )
        if dirpath:
            self.output_dir.set(dirpath)
            self.update_message(f"已设置输出目录: {dirpath}")
    
    def get_output_path(self) -> Path:
        """Generate output file path based on input files"""
        output_dir = Path(self.output_dir.get())
        keyword_file = Path(self.keyword_file.get())
        return output_dir / f"{keyword_file.stem}_分类结果.xlsx"
    
    def update_message(self, msg: str):
        """Update message display with new message"""
        self.msg_text.config(state=tk.NORMAL)
        self.msg_text.insert(tk.END, msg + "\n")
        self.msg_text.see(tk.END)
        self.msg_text.config(state=tk.DISABLED)
        self.root.update()
    
    def update_progress(self, current: int, total: int):
        """Update progress bar"""
        self.progress["maximum"] = total
        self.progress["value"] = current
        self.root.update()
    
    def run_classification(self):
        """Run the classification process"""
        if not self.rule_file.get() or not self.keyword_file.get():
            messagebox.showerror("错误", "请选择规则文件和关键词文件")
            return
        
        try:
            # Custom tqdm that updates the GUI
            class GUItqdm(tqdm):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    self.gui = kwargs.pop('gui', None)
                
                def update(self, n=1):
                    if self.gui:
                        self.gui.update_progress(self.n, self.total)
                    return super().update(n)
            
            # Generate output path
            output_path = Path(self.output_dir.get())
            message.debug(f"正在生成分类结果文件路径: {output_path}")
            
            # Create file info objects
            keyword_file_info = FileInfo(
                file_path=Path(self.keyword_file.get()),
                sheet_name="Sheet1",
                file_name='待分类关键词'
            )
            work_flow_file_info = FileInfo(
                file_path=Path(self.rule_file.get()),
                file_name='工作流规则'
            )
            
            # Read files
            self.update_message(f"正在读取关键词文件...{keyword_file_info}")
            keywords = read_keywords(keyword_file_info, 1)
            
            self.update_message(f"正在读取工作流规则...{work_flow_file_info}")
            work_flow_rules = read_work_flow_rules(work_flow_file_info)
            
            # Create classifier
            classifier = KeywordClassifier(
                case_sensitive=self.case_sensitive.get(),
                separator="&"
            )
            
            rule_level = 1
            classified_keywords = ClassifiedKeywordDTO()
            
            # Process classification
            while rule_level <= work_flow_rules.max_level:
                self.update_message(f"正在处理规则级别 {rule_level}...")
                work_flow_stage_rule = work_flow_rules.filter(rule_level=rule_level)
                classifier.set_rules(work_flow_stage_rule)
                message.debug(f"设置规则完成")
                classified_keywords = classifier.classify_keywords(keywords)
                message.debug(f"分词完成")
                rule_level += 1
                if rule_level <= work_flow_rules.max_level:
                    keywords = trans_classified_keyword_to_next_source_keyword(classified_keywords)
                message.debug(f"设置下一层级关键词完成")
            # Save results
            time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            if classified_keywords.data:
                save_classified_keywords(
                    classified_keywords,
                    time_str=time_str,
                    is_create_new_file=True,
                    output_dir=output_path
                )
                self.update_message(f"分类结果已保存到: {output_path}")
                messagebox.showinfo("完成", f"关键词分类已完成！\n结果保存在: {output_path}")
            else:
                self.update_message("没有匹配到关键词")
                messagebox.showinfo("完成", "没有匹配到关键词")
            
        except Exception as e:
            self.update_message(f"发生错误: {str(e)}")
            messagebox.showerror("错误", f"处理过程中发生错误:\n{str(e)}")

def main():
    
    app = KeywordClassifierGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
