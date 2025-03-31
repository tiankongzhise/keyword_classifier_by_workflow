# 关键词分类器（基于工作流）

## 项目简介

这是一个基于工作流的关键词分类器，可以根据预定义的规则对关键词进行多级分类。系统支持多阶段分类流程，每个阶段可以基于前一阶段的结果进行进一步细分，形成层级化的分类结果。

## 功能特点

- 支持多级分类工作流（最多支持4级分类）
- 强大的规则匹配引擎，支持AND、OR、NOT等逻辑操作
- 自动处理不可见字符和重复项
- 结果保存为Excel格式，便于查看和后续处理
- 完善的错误处理和日志记录

## 项目结构

```
.
├── data/                  # 数据目录，存放规则文件和待分类文件
├── src/                   # 源代码
│   └── kw_cf/             # 关键词分类器模块
│       ├── excel_handler.py       # Excel文件处理
│       ├── keyword_classifier.py  # 关键词分类引擎
│       ├── main.py               # 主程序入口
│       ├── models.py             # 数据模型定义
│       └── workflow_processor.py # 工作流处理器
├── test/                  # 测试代码
│   └── test.py            # 测试脚本
└── 工作流结果/             # 默认输出目录
```

## 使用方法

### 准备数据文件

1. 待分类文件：命名格式为`待分类_*.xlsx`，必须包含`关键词`列
2. 工作流规则文件：命名格式为`工作流规则_*.xlsx`，必须包含以下工作表：
   - Sheet1：包含`分类规则`和`结果文件名称`列
   - Sheet2（可选）：包含`分类规则`、`结果文件名称`和`分类sheet名称`列
   - Sheet3及以上（可选）：额外包含`上层分类规则`列

### 运行程序

```python
from src.kw_cf.workflow_processor import WorkFlowProcessor
from pathlib import Path

# 初始化工作流处理器
processor = WorkFlowProcessor()

# 处理工作流
processor.process_workflow(
    rules_path=Path('data/工作流规则_1.xlsx'),
    keywords_path=Path('data/待分类_1.xlsx')
)
```

## 规则语法

分类规则支持以下语法：

- `A+B`：同时包含A和B（AND操作）
- `A|B`：包含A或B（OR操作）
- `<A>`：不包含A（NOT操作）
- `A<B>`：包含A但不包含B
- `[A]`：精确匹配A
- `(A+B)|C`：组合逻辑，包含A和B，或者包含C

## 开发指南

### 环境设置

```bash
# 安装依赖
pip install -r requirements.txt
```

### 运行测试

```bash
python -m test.test
```

