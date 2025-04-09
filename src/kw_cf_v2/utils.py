from typing import List,Tuple,Set,Any
from .message import message
from .models import ClassifiedKeyword
def preprocess_text(text, error_callback=None):
    """预处理文本，清除不可见的干扰字符
    Args:
        text: 需要预处理的文本
        error_callback: 错误回调函数，用于将错误信息传递给UI显示
    Returns:
        清除干扰字符后的文本
    """

    if not text:
        return text

    # 定义需要清除的不可见字符列表

    invisible_chars = [
        0x200B,  # 零宽空格
        0x200C,  # 零宽非连接符
        0x200D,  # 零宽连接符
        0x200E,  # 从左至右标记
        0x200F,  # 从右至左标记
        0x202A,  # 从左至右嵌入
        0x202B,  # 从右至左嵌入
        0x202C,  # 弹出方向格式
        0x202D,  # 从左至右覆盖
        0x202E,  # 从右至左覆盖
        0x2060,  # 单词连接符
        0x2061,  # 函数应用
        0x2062,  # 隐形乘号
        0x2063,  # 隐形分隔符
        0x2064,  # 隐形加号
        0xFEFF,  # 零宽非断空格(BOM)
    ]

    """清理规则中的不可见字符并检查编码"""

    # 检查是否包含零宽空格等不可见字符

    has_invisible = False

    cleaned_rule = ""

    for char in text:
        code_point = ord(char)

        if code_point in invisible_chars:
            msg = f"发现不可见字符: U+{code_point:04X} 在规则 '{text}' 中"

            message.info(msg)

            if error_callback:
                error_callback(msg)

            has_invisible = True

            # 不添加这个字符到清理后的规则

        else:
            cleaned_rule += char

    # 如果规则被清理了，打印出来

    if has_invisible:
        msg1 = f"清理前: '{text}' (长度: {len(text)})"

        msg2 = f"清理后: '{cleaned_rule}' (长度: {len(cleaned_rule)})"

        message.info(msg1)

        message.info(msg2)
    return cleaned_rule if has_invisible else text


def preserve_order_deduplicate(lst: List[str]) -> List[str]:
    """保序去重函数（兼容Python 3.6+）"""

    seen = set()

    return [x for x in lst if not (x in seen or seen.add(x))]


def processing_pipeline(item_list:List|Tuple|Set) -> List[str]:
        """处理流水线：类型转换 -> 预处理 -> 空值过滤 -> 保序去重"""

        # 类型安全转换

        if not isinstance(item_list, (list, tuple, set)):
            raise ValueError("输入必须是可迭代对象")

        # 保留原始数据

        new_item_list = [str(item) for item in item_list]


        processed = [
            preprocess_text(item).strip()  # 移除首尾空格
            for item in new_item_list
        ]

        # 空值过滤（包括空白字符）

        non_empty = [keyword for keyword in processed if keyword]

        # 保序去重逻辑

        return preserve_order_deduplicate(non_empty)


def processing_keyword(keyword:Any) -> str:
    """处理关键词：类型转换 -> 预处理"""
    new_keyword = str(keyword)
    new_keyword = preprocess_text(new_keyword).strip()
    return new_keyword
    

def fomart_classified_keywords_to_dict(classified_keyword:ClassifiedKeyword) -> dict:
    """格式化分类结果"""
    temp_dict = {}
    temp_dict['关键词'] = classified_keyword.keyword
    for key,value in classified_keyword.matched_info.items():
        temp_dict[key] = value
    return {
        'output_file_name':classified_keyword.output_file_name,
        'output_sheet_name':classified_keyword.output_sheet_name,
        'source_file_path':classified_keyword.source_file_Path,
        'process_level':classified_keyword.process_level,
        'data':temp_dict
    }
