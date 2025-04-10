from typing import List,Tuple,Set,Any
from .message import message
from .models import ClassifiedKeyword,WorkFlowRuleDTO,SourceKeyword,ClassifiedKeywordDTO,SourceKeywordDTO
from copy import deepcopy
import sys
from pathlib import Path

def get_exe_dir():
    """获取可执行文件所在目录"""
    if getattr(sys, 'frozen', False):
        # 打包后的可执行文件目录
        return Path(sys.executable).parent
    else:
        # 开发环境的脚本目录
        return Path(__file__).parent
def get_func_env():
    """获取运行环境"""
    if getattr(sys, 'frozen', False):
        # 打包后的可执行文件目录
        return 'exe'
    else:
        # 开发环境的脚本目录
        return 'py'



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

def trans_work_flow_rules_to_dict(workflow_rules:WorkFlowRuleDTO) -> dict:
    """格式化分类结果"""
    result_dict = {}
    for workflow_rule in workflow_rules.data:
        result_dict.setdefault(workflow_rule.rule_level,{}).setdefault(workflow_rule.rule.lower(),{}).setdefault(workflow_rule.target_file_name,{}).setdefault(workflow_rule.target_sheet_name,[]).append({'limit_last_matched_rule':workflow_rule.limit_last_matched_rule,'rule_tag':workflow_rule.rule_tag})
    
    err_list = []
    for level,rule_rule_dict in result_dict.items():
        for rule,target_file_dict in rule_rule_dict.items():
            for file_name,target_sheet_dict in target_file_dict.items():
                for sheet_name,info in target_sheet_dict.items():
                    if len(info) > 1:
                        err_list.append(f"规则{rule}在等级{level},文件{file_name}的{sheet_name}中存在重复的匹配规则{info}")
    if err_list:
        message.error('\n'.join(err_list))
        raise 
    
    return result_dict
def get_target_file_name(rules_map:dict,process_level:int,matched_rule:str,keyword:SourceKeyword) -> str:
    """获取目标文件名"""
    rsp = list(rules_map.get(process_level,{}).get(matched_rule.lower(),{}).keys())
    if rsp:
        if rsp[0] == '全':
            return keyword.source_file_name
        else:
            return rsp[0]
    else:
        # raise ValueError(f"get_target_file_name出现意料外的错误,规则{matched_rule}在等级{process_level}中不存在")
        return ''
def get_target_sheet_name(rules_map:dict,process_level:int,matched_rule:str,target_file_name:str,keyword:SourceKeyword) -> str:
    """获取目标sheet名"""
    if process_level == 1:
        return 'Sheet1'
    rsp = list(rules_map.get(process_level,{}).get(matched_rule.lower(),{}).get(target_file_name,{}).keys())
    rsp1 = list(rules_map.get(process_level,{}).get(matched_rule.lower(),{}).get('全',{}).keys())
    if rsp:
        if rsp[0] == '全':
            return keyword.source_sheet_name
        else:
            return rsp[0]
    elif rsp1:
        if rsp1[0] == '全':
            return keyword.source_sheet_name
        else:
            return rsp1[0]
    else:
        # raise ValueError(f"get_target_sheet_name出现意料外的错误,规则{matched_rule}在等级{process_level}的文件{target_file_name}中不存在")
        return ''

def create_classified_keyword(new_keyword:str,keyword:SourceKeyword,matched_rule:str,process_level:int,target_file_name:str,target_sheet_name:str,matched_info:dict) -> ClassifiedKeyword:
    """创建分类关键词对象"""
    temp_item = ClassifiedKeyword()
    temp_item.keyword = new_keyword
    temp_item.source_file_name = keyword.source_file_name
    temp_item.source_sheet_name = keyword.source_sheet_name
    temp_item.source_file_Path = keyword.source_file_Path
    temp_item.process_level = process_level
    temp_item.output_file_name = target_file_name
    temp_item.output_sheet_name = target_sheet_name
    temp_item.matched_info = deepcopy(keyword.matched_info)
    if process_level >=3:
        new_matched_rule_col_name = f'阶段{process_level}匹配规则'
        new_matched_rule_tag_col_name = f'阶段{process_level}匹配标签'
    else:
        new_matched_rule_col_name = '匹配规则'
        new_matched_rule_tag_col_name = '匹配标签'
    temp_item.matched_info[new_matched_rule_col_name] = matched_rule
    temp_item.matched_info[new_matched_rule_tag_col_name] = matched_info.get('rule_tag','')
    return temp_item

def trans_classified_keyword_to_next_source_keyword(old_data:ClassifiedKeywordDTO)->SourceKeywordDTO:
    result = SourceKeywordDTO()
    for item in old_data.data:
        result.data.append(
            SourceKeyword(
                keyword=item.keyword,
                source_file_name=item.output_file_name,
                source_sheet_name=item.output_sheet_name,
                source_file_Path=item.source_file_Path,
                process_level=item.process_level+1,
                matched_info=item.matched_info
                ))
    return result

def is_matched(rules:dict,keyword:SourceKeyword,matched_rule:str)->bool|List:
    
    
    if keyword.process_level == 2:
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,None):
            return True
        return False

    elif keyword.process_level == 3:
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get(keyword.source_sheet_name,None):
            return True
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get('全',None):
            return True
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get(keyword.source_sheet_name,None):
            return True
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get('全',None):
            return True
        return False
    elif keyword.process_level >3:
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get(keyword.source_sheet_name,None)
        if temp_info:
            return temp_info
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get('全',None)
        if temp_info:
            return temp_info
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get(keyword.source_sheet_name,None)
        if temp_info:
            return temp_info
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get('全',None)
        if temp_info:
            return temp_info
        return False
    else:
        raise Exception(f'utils->is_matched要求keyword的proce_level>=3,{keyword},matched_rule:{matched_rule}')

def get_rules_tag_info(rules:dict,keyword:SourceKeyword,matched_rule:str)->dict:
    if keyword.process_level == 3:
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get(keyword.source_sheet_name,None):
            return rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get(keyword.source_sheet_name,None)[0]
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get('全',None):
            return rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get('全',None)[0]
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get(keyword.source_sheet_name,None):
            return rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get(keyword.source_sheet_name,None)[0]
        if rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get('全',None):
            return rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get('全',None)[0]
        raise Exception(f'utlis->get_rules_tag_info,应该不存在无法找到的情况,keyword:{keyword},matched_rule:{matched_rule}')
    elif keyword.process_level >3:
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get(keyword.source_file_name,{}).get(keyword.source_sheet_name,None)
        if temp_info:
            return temp_info[0]
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get(keyword.source_sheet_name,None)
        if temp_info:
            return temp_info[0]
        temp_info = rules.get(keyword.process_level,{}).get(matched_rule,{}).get('全',{}).get('全',None)
        if temp_info:
            return temp_info[0]
        raise Exception(f'utlis->get_rules_tag_info,应该不存在无法找到的情况,keyword:{keyword},matched_rule:{matched_rule}')
    else:
        raise Exception(f'utils->is_matched要求keyword的proce_level>=3,{keyword},matched_rule:{matched_rule}')

def get_last_level_rule(rules:dict,level:int,matched_rule:str,file_name:str,sheet_name:str,keyword:SourceKeyword)->tuple:
    rule1 = rules.get(level,{}).get(matched_rule,{}).get(file_name,{}).get(sheet_name,None)
    if rule1:
        if rule1[0]['limit_last_matched_rule'] == '全':
            return keyword.matched_info.get(f'阶段{keyword.process_level-1}匹配规则',''),rule1[0]['rule_tag']
        else:
            return rule1[0]['limit_last_matched_rule'],rule1[0]['rule_tag']
    rule2 = rules.get(level,{}).get(matched_rule,{}).get(file_name,{}).get('全',None)
    if rule2:
        if rule2[0]['limit_last_matched_rule'] == '全':
            return keyword.matched_info.get(f'阶段{keyword.process_level-1}匹配规则',''),rule2[0]['rule_tag']
        else:
            return rule2[0]['limit_last_matched_rule'],rule2[0]['rule_tag']
    rule3 = rules.get(level,{}).get(matched_rule,{}).get('全',{}).get(sheet_name,None)
    if rule3:
        if rule3[0]['limit_last_matched_rule'] == '全':
            return keyword.matched_info.get(f'阶段{keyword.process_level-1}匹配规则',''),rule3[0]['rule_tag']
        else:
            return rule3[0]['limit_last_matched_rule'],rule3[0]['rule_tag']
    rule4 = rules.get(level,{}).get(matched_rule,{}).get('全',{}).get('全',None)
    if rule4:
        if rule4[0]['limit_last_matched_rule'] == '全':
            return keyword.matched_info.get(f'阶段{keyword.process_level-1}匹配规则',''),rule4[0]['rule_tag']
        else:
            return rule4[0]['limit_last_matched_rule'],rule4[0]['rule_tag']
    return '',''
    
    
