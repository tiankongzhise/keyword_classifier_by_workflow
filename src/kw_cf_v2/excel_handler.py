from .message import message
from .models import SourceKeyword,SourceKeywordDTO,FileInfo,WorkFlowRule,WorkFlowRuleDTO,ClassifiedKeywordDTO,ClassifiedKeyword
from typing import cast
import pandas as pd
from pathlib import Path
from .utils import fomart_classified_keywords_to_dict
from .utils import processing_keyword
from openpyxl import load_workbook
def read_keywords(file_info:FileInfo,level:int) -> SourceKeywordDTO:
    file_name = file_info.file_name
    sheet_name = file_info.sheet_name
    file_path = file_info.file_path
    df = pd.read_excel(file_path,sheet_name=sheet_name,keep_default_na=False)
    records = df.to_dict(orient='records')
    result = []
    for item in records:
        if not item.get('关键词'):
            message(f"excel{file_path}中{sheet_name}工作表没有关键词列，请检查")
            raise
        temp_item = SourceKeyword()
        temp_item.keyword = processing_keyword(item.pop('关键词'))
        temp_item.source_file_name = file_name
        temp_item.source_sheet_name = sheet_name
        temp_item.source_file_Path = file_path
        temp_item.process_level = level
        temp_item.matched_info = item
        result.append(temp_item)
    rsp = SourceKeywordDTO(data=result)
    message(f"{file_name}中{sheet_name}工作表读取成功,file_path:{file_path}")
    return rsp

def level_map(sheet_name:str):
    level = int(sheet_name.split('Sheet')[1])
    return level
    
        
def read_work_flow_rules(file_info:FileInfo):
    file_name = file_info.file_name
    file_path = file_info.file_path
    
    # 读取Excel文件的所有sheet
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    result = []
    max_level = 0
    for sheet_name in sheet_names:
        df = pd.read_excel(file_path,sheet_name=sheet_name)
        records = df.to_dict(orient='records')
        for record in records:
            temp_item = WorkFlowRule()
            temp_item.rule_level = level_map(cast(str,sheet_name))
            temp_item.rule = processing_keyword(record.pop('分类规则'))
            temp_item.target_file_name = processing_keyword(record.pop('结果文件名称'))
            temp_item.target_sheet_name = processing_keyword(record.pop('分类sheet名称') if record.get('分类sheet名称') else 'Sheet1')
            temp_item.limit_last_matched_rule = processing_keyword(record.pop('上层分类规则') if record.get('上层分类规则') else '')
            temp_item.rule_tag = processing_keyword(record.pop('分类标签') if record.get('分类标签') else '')
            result.append(temp_item)
            if temp_item.rule_level > max_level:
                max_level = temp_item.rule_level
    rsp = WorkFlowRuleDTO(data=result,max_level=max_level)
    message(f"工作流规则{file_name}全部sheet规则读取完毕,共计{len(sheet_names)}个sheet,file_path:{file_path},sheet_names:{sheet_names}")
    return rsp
    

def set_file_name(source_file_path:Path,
                  process_level:int,
                  output_file_name:str,
                  time_str:str,
                  is_create_new_file:bool=False)->Path:
    if process_level == 1:
        return Path(__file__).parent.parent.parent / f"工作流结果/{output_file_name}_{time_str}.xlsx"
    if is_create_new_file:
        return Path(__file__).parent.parent.parent / f"工作流结果/{output_file_name}_{time_str}.xlsx"
    else:
        return source_file_path


def save_classified_keywords(classified_keywords:ClassifiedKeywordDTO,time_str:str,is_create_new_file:bool=False):
    result = {}
    file_path_map = {}
    output_dir = Path(__file__).parent.parent.parent / "工作流结果"
    if not output_dir.exists(): 
        output_dir.mkdir()
    for classified_keyword in classified_keywords.data:
        temp_dict = fomart_classified_keywords_to_dict(classified_keyword)
        result.setdefault(temp_dict['output_file_name'],{}).setdefault(temp_dict['output_sheet_name'],[]).append(temp_dict['data'])
        if temp_dict['output_file_name'] not in file_path_map:
            file_path_map[temp_dict['output_file_name']] = set_file_name(temp_dict['source_file_path'],
                                                                         temp_dict['process_level'],
                                                                         temp_dict['output_file_name'],
                                                                         time_str,
                                                                         is_create_new_file
                                                                         )
        else:
            if file_path_map[temp_dict['output_file_name']] != classified_keyword.source_file_Path and not is_create_new_file and temp_dict['process_level']!=1:
                raise ValueError(f"异常情况save_classified_keywords,output_file_name:{temp_dict['output_file_name']}存在多个保持文件路径{classified_keyword.source_file_Path}与{file_path_map[temp_dict['output_file_name']]}!")
        
    for output_file_name,sheet_dict in result.items():
        file_path:Path = file_path_map[output_file_name]
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            for sheet_name,data in sheet_dict.items():
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            message.info(f"保存{output_file_name}的分类结果成功共保存了{len(sheet_dict.items())}个sheet,文件路径:{file_path_map[output_file_name]}")

            
  