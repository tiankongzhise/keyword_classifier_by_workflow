import logging
import sys
from typing import Callable, Optional, Dict, Any

# 创建根日志器
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 创建控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# 创建日志格式
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
console_handler.setFormatter(formatter)

# 添加处理器到日志器
logger.addHandler(console_handler)

# UI日志处理器类
class UILogHandler(logging.Handler):
    """将日志消息转发到UI界面的处理器"""
    
    def __init__(self, callback: Callable[[str, str], None], level=logging.INFO):
        """初始化UI日志处理器
        
        Args:
            callback: 回调函数，接收日志级别和消息作为参数
            level: 日志级别，默认为INFO
        """
        super().__init__(level)
        self.callback = callback
        self.setFormatter(formatter)
    
    def emit(self, record):
        """发送日志记录到UI"""
        try:
            msg = self.format(record)
            self.callback(record.levelname, msg)
        except Exception:
            self.handleError(record)
    
    def set_level(self, level):
        """设置日志级别"""
        self.setLevel(level)

# 存储所有UI日志处理器的字典
ui_handlers: Dict[str, UILogHandler] = {}

def add_ui_handler(callback: Callable[[str, str], None], name: str = "default", level=logging.INFO) -> UILogHandler:
    """添加UI日志处理器到根日志器
    
    Args:
        callback: 回调函数，接收日志级别和消息作为参数
        name: 处理器名称，用于后续引用
        level: 日志级别，默认为INFO
        
    Returns:
        添加的UI日志处理器实例
    """
    handler = UILogHandler(callback, level)
    logger.addHandler(handler)
    ui_handlers[name] = handler
    return handler

def remove_ui_handler(name: str = "default") -> bool:
    """移除UI日志处理器
    
    Args:
        name: 处理器名称
        
    Returns:
        是否成功移除
    """
    if name in ui_handlers:
        logger.removeHandler(ui_handlers[name])
        del ui_handlers[name]
        return True
    return False

def set_ui_handler_level(level, name: str = "default") -> bool:
    """设置UI日志处理器的级别
    
    Args:
        level: 日志级别
        name: 处理器名称
        
    Returns:
        是否成功设置
    """
    if name in ui_handlers:
        ui_handlers[name].set_level(level)
        return True
    return False

def get_logger(name=None):
    """获取日志器"""
    return logging.getLogger(name) if name else logger
