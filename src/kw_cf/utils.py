from . import models
from typing import List,TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from .workflow_processor_copy import WorkFlowProcessor

class WorkFlowProcessorUtil(object):
    def __init__(self,parent_self:'WorkFlowProcessor'):
        self.parent_self = parent_self
        self.error_callback = self.parent_self.error_callback
        
    @staticmethod
    def transfrom_unmathced_keywords(unmatched_keywords:List[models.UnMatchedKeyword])->pd.DataFrame:
        result = []
        for unmatched_keyword in unmatched_keywords:
            temp_dict = {}
            temp_dict['关键词'] = unmatched_keyword.keyword
            temp_dict['分类层级'] = unmatched_keyword.level
            temp_dict['来源sheet'] = unmatched_keyword.source_sheet_name
            result.append(temp_dict)
        return pd.DataFrame(result)
    @staticmethod
    def transfrom_classified_keywords(classified_keywords:List[models.ClassifiedKeyword])->pd.DataFrame:
        result = []
        for classified_keyword in classified_keywords:
            temp_dict = {}
            temp_dict['关键词'] = classified_keyword.keyword
            temp_dict['匹配的规则'] = classified_keyword.matched_rule
            if classified_keyword.level >2:
                temp_dict[classified_keyword.parent_rule_column] = classified_keyword.parent_rule
                temp_dict[classified_keyword.rule_tag_column] = classified_keyword.rule_tag
            result.append(temp_dict)
        return pd.DataFrame(result)
                
            
        
  
    def transform_to_df(self,data:List[models.UnMatchedKeyword|models.ClassifiedKeyword])->pd.DataFrame:
        map_func = {
            models.UnMatchedKeyword:self.transfrom_unmathced_keywords,
            models.ClassifiedKeyword:self.transfrom_classified_keywords
        }
        return map_func[type(data[0])](data)
        

    def trans_words_to_cassified_result(self,classify_result:List[models.ClassifiedWord],workflow_rules:models.WorkFlowRules)->models.ClassifiedResult:
        classified_keywords = []
        unclassified_keywords = []
        try:
            # 处理分类结果
            for temp in classify_result:
                keyword = temp.keyword
                matched_rules = temp.matched_rule

  
                temp_dict = {}
                

                
                if matched_rules:
                    # 确定映射属性
                    rule_item = workflow_rules.filter_rules(rule=matched_rules)
                    if rule_item is None:
                        msg = f'_trans_words_to_cassified_result匹配异常，关键词匹配了一个分类工作流中不存在的规则。temp:{temp},workflow_rules:{workflow_rules}'
                        if self.error_callback:
                            self.error_callback(msg)
                        raise Exception(msg)
                    rule_item = rule_item.rules
                    if rule_item == []:
                        msg = f'_trans_words_to_cassified_result匹配异常，关键词匹配了一个分类工作流中不存在的规则。temp:{temp},workflow_rules:{workflow_rules}'
                        if self.error_callback:
                            self.error_callback(msg)
                        raise Exception(msg)
                    rule_item = rule_item[0]
                    workflow_level = rule_item.level
                    output_name = rule_item.output_name
                    classified_sheet_name = rule_item.classified_sheet_name
                    parent_rule = rule_item.parent_rule
                    rule_tage = rule_item.rule_tag
                    parent_rule_column = f'阶段{workflow_level-1}父级规则'
                    rule_tag_column = f'阶段{workflow_level}规则标签'
                    #创建映射字典
                    temp_dict = {
                        'level':workflow_level,
                        'keyword':keyword,
                        'matched_rule':matched_rules,
                        'output_name':output_name,
                        'classified_sheet_name':classified_sheet_name,
                        'parent_rule':parent_rule,
                        'rule_tage':rule_tage,
                        'parent_rule_column':parent_rule_column,
                        'rule_tag_column':rule_tag_column,
                    }
                    classified_keywords.append(
                        models.ClassifiedKeyword(**temp_dict))
                else:
                    # 创建映射关系
                    source_sheet_name = temp.source_sheet_name
                    souce_file_name = temp.source_file_name
                    process_level = temp.level
                    temp_dict = {
                        'level':process_level,
                        'keyword':keyword,
                        'output_name':souce_file_name or '未分类关键词',
                        'classified_sheet_name':'Sheet1' if souce_file_name is None else '未匹配关键词' ,
                        'source_sheet_name':source_sheet_name or 'Sheet1'
                    }
                    unclassified_keywords.append(
                            models.UnMatchedKeyword( **temp_dict))

            return models.ClassifiedResult(classified_keywords=classified_keywords,unclassified_keywords=unclassified_keywords)
        except Exception as e:
            msg = f"分类结果转换出错: {e},\nworkflow_rules: {workflow_rules},\nclassified_keywords:{classified_keywords},\nunclassified_keywords:{unclassified_keywords}"
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg) from e
        
