from .core import KeywordClassifier
from .excel_handler import read_keywords,read_work_flow_rules,save_classified_keywords
from .message import message
from .models import FileInfo
from .models import WorkFlowRule,ClassifiedKeywordDTO
from .utils import trans_classified_keyword_to_next_source_keyword

from datetime import datetime
from pathlib import Path

def main():
    keyword_file_path = Path(__file__).parent.parent.parent /"data/待分类_百度提示词.xlsx"
    keyword_file_info = FileInfo(file_path=keyword_file_path,sheet_name="Sheet1",file_name='待分类关键词')
    work_flow_file_path = Path(__file__).parent.parent.parent /"data/工作流规则_java类分词.xlsx"
    work_flow_file_info = FileInfo(file_path=work_flow_file_path,file_name='工作流规则')
    
    
    # 读取关键词
    keywords = read_keywords(keyword_file_info,1)
    # 读取工作流规则
    work_flow_rules = read_work_flow_rules(work_flow_file_info)
    # 创建分类器
    classifier = KeywordClassifier(case_sensitive=False,separator="&")
    rule_level = 1
    classified_keywords = ClassifiedKeywordDTO()


    while True:
        if rule_level > work_flow_rules.max_level:
            break

        work_flow_stage_rule = work_flow_rules.filter(rule_level=rule_level)
        classifier.set_rules(work_flow_stage_rule)
        classified_keywords = classifier.classify_keywords(keywords)
        
        # for classified_keyword in classified_keywords.data:
        #     if classified_keyword.keyword == '培训数据库编程':
        #         print(f'\nwhile中 {classified_keyword}\n')
        
        # 检验数据是否丢失
        # if rule_level == 1:
        #     time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        #     if classified_keywords.data:
        #         save_classified_keywords(classified_keywords,time_str=time_str,is_create_new_file=True)
        
        rule_level += 1
        if rule_level<=work_flow_rules.max_level:
            keywords = trans_classified_keyword_to_next_source_keyword(classified_keywords)

    time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    if classified_keywords.data:
        save_classified_keywords(classified_keywords,time_str=time_str,is_create_new_file=True)
    else:
        message("没有匹配到关键词")
