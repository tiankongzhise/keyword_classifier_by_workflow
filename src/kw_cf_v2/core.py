from lark import Lark, Transformer, v_args
from typing import List, Optional, Callable
from .models import WorkFlowRuleDTO,SourceKeyword,SourceKeywordDTO,ClassifiedKeyword,ClassifiedKeywordDTO,WorkFlowRule
from .message import message
from tqdm import tqdm
from .utils import (processing_pipeline,
                    processing_keyword,
                    trans_work_flow_rules_to_dict,
                    create_classified_keyword,
                    get_target_file_name,
                    get_target_sheet_name,
                    is_matched,
                    get_rules_tag_info,
                    get_last_level_rule)
from copy import deepcopy

class KeywordClassifier:
    def __init__(self, case_sensitive=False, separator="&"):
        self.rules = {}
        self.parsed_rules = []
        self.case_sensitive = case_sensitive
        self.separator = separator
        self.process_level:int = -1
        self.parser = self._create_parser()

    def _create_parser(self):
        """创建Lark解析器"""
        grammar = r"""
            ?start: expr
            
            ?expr: or_expr
            
            ?or_expr: and_expr
                   | or_expr "|" and_expr -> or_op
            
            ?and_expr: atom
                    | and_expr "+" atom -> and_op
            
            ?atom: exact
                 | term_exclude
                 | term
                 | "(" expr ")" -> group
            
            term_exclude: WORD "<" expr ">" -> term_exclude_match
            
            exact: "[" WORD "]" -> exact_match
            exclude: "<" expr ">" -> exclude_match
            term: WORD -> simple_term
            
            WORD: /[^\[\]<>|+()\s]+/
            
            %import common.WS
            %ignore WS
        """
        return Lark(grammar, parser="lalr")

    @v_args(inline=True)
    class RuleTransformer(Transformer):
        """转换解析树为可执行的匹配函数"""

        def __init__(self, case_sensitive=False):
            super().__init__()

            self.case_sensitive = case_sensitive

        def or_op(self, left, right):
            return lambda keyword: left(keyword) or right(keyword)

        def and_op(self, left, right):
            return lambda keyword: left(keyword) and right(keyword)

        def group(self, expr):
            return expr

        def exact_match(self, word):
            word_str = str(word)

            if self.case_sensitive:
                return lambda keyword: keyword == word_str
            else:
                return lambda keyword: keyword.lower() == word_str.lower()

        def exclude_match(self, expr):
            return lambda keyword: not expr(keyword)

        def term_exclude_match(self, term, expr):
            term_str = str(term)

            if self.case_sensitive:
                return lambda keyword: term_str in keyword and not expr(keyword)
            else:
                return lambda keyword: term_str.lower() in keyword.lower() and not expr(
                    keyword
                )

        def simple_term(self, word):
            word_str = str(word)

            if self.case_sensitive:
                return lambda keyword: word_str in keyword
            else:
                # 修复单个字符匹配逻辑 - 移除冗余条件

                return lambda keyword: word_str.lower() in keyword.lower()

    def set_rules(self, rules: WorkFlowRuleDTO, error_callback=None):
        """设置分词规则
        Args:
            rules: 规则列表
            error_callback: 错误回调函数，用于将错误信息传递给UI显示
        """
        
        processed_rules = processing_pipeline(rules.value_list_by_field_name(field_name='rule'))
        rule_level = rules.value_list_by_field_name(field_name='rule_level')
        if len(rule_level)>1:
            message.error(f'set_rules->rule_level:{rule_level}存在多个层级规则')
            raise ValueError("存在多个层级的规则,非预期情况")
        self.process_level = list(rule_level)[0]
        
        self.rules = trans_work_flow_rules_to_dict(rules)

        self.parsed_rules = []

        parse_errors = []

        # 解析每条规则

        for i, rule in enumerate(processed_rules):
            try:
                tree = self.parser.parse(rule)

                transformer = self.RuleTransformer(self.case_sensitive)

                matcher = transformer.transform(tree)

                self.parsed_rules.append((rule, matcher))

            except Exception as e:
                error_msg = f"规则 '{rule}' 解析失败: {str(e)}"

                parse_errors.append(error_msg)

                message.error(error_msg)

                # 如果提供了错误回调函数，则调用它

                if error_callback:
                    error_callback(error_msg)

        return parse_errors  # 返回解析错误列表

    def pre_check(self,keywords:SourceKeywordDTO) -> None:
        k1 = keywords.value_list_by_field_name('keyword')
        k2 = processing_pipeline(k1)
        if len(keywords.data) != len(k1):
            temp_list = []
            count_list = []
            for temp in keywords.data:
                if temp.keyword not in temp_list:
                    temp_list.append(temp.keyword)
                else:
                    count_list.append(temp.keyword)
            message.error(f"{self.__class__.__name__}->classify_keywords:存在重复关键词{','.join(count_list)}")
            raise
        if len(k1) != len(k2):
            temp_list = [k1_item for k1_item in k1 if k1_item not in k2]
            message.error(f"{self.__class__.__name__}->classify_keywords:部分关键词清洗后不存在,请检查!{','.join(temp_list)}")  
        if len(k2) != len(set(k2)):
            temp_list = []
            count_list = []
            for temp in k2:
                if temp not in temp_list:
                    temp_list.append(temp)
                else:
                    count_list.append(temp)
            message.error(f"{self.__class__.__name__}->classify_keywords:清洗后存在重复关键词,请检查!{','.join(count_list)}")
        
    def classify_keywords(self, keywords: SourceKeywordDTO)->ClassifiedKeywordDTO:
        """对关键词进行分类（单进程版本）"""
        #对关键词进行检查 避免重复
        self.pre_check(keywords)
        result = ClassifiedKeywordDTO()
        for source_keyword in tqdm(keywords.data,desc=f'正在进行{self.process_level}阶段的分词'):
            result.data.append(self.classify_keyword(source_keyword))
            
        return result
    def classify_keyword(self, keyword: SourceKeyword)->ClassifiedKeyword:
        new_keyword = processing_keyword(keyword.keyword)
        matched_rule = ''
        matched_tag = False
        #多阶段容错 未分类直接返回
        if keyword.source_file_name == '未匹配任何规则':
            return create_classified_keyword(new_keyword=new_keyword,
                                keyword=keyword,
                                matched_rule='',
                                process_level=keyword.process_level,
                                target_file_name='未匹配任何规则',
                                target_sheet_name='Sheet1',
                                matched_info=keyword.matched_info)
        



        for rule_text, rule_matcher in self.parsed_rules:
            matched_tag = False
            local_matched_rule = ''
            target_file_name = ''
            target_sheet_name = ''
            temp_last_level_rule = ''
            temp_last_level_rule_tag =''
            if not rule_matcher(new_keyword):
                continue 
            matched_rule = rule_text
            local_matched_rule:str = deepcopy(rule_text)
            local_matched_rule = local_matched_rule.lower()
            
            if keyword.process_level == 1:
                target_file_name = get_target_file_name(self.rules,keyword.process_level,local_matched_rule,keyword)
                target_sheet_name = 'Sheet1'
                matched_tag = True
                break            
            temp_file_name = get_target_file_name(self.rules,keyword.process_level,local_matched_rule,keyword)
            if keyword.source_file_name != temp_file_name:
                continue
            target_file_name = temp_file_name
            temp_sheet_name = get_target_sheet_name(self.rules,keyword.process_level,local_matched_rule,target_file_name,keyword)
            if keyword.process_level == 2:
                if temp_sheet_name:
                    target_sheet_name = temp_sheet_name
                    matched_tag = True
                break
            if keyword.source_sheet_name != temp_sheet_name:
                continue
            target_sheet_name = temp_sheet_name
            
            if target_sheet_name:
                if keyword.process_level < 4:
                    matched_tag = True
                    break
                last_matched_rule_col_name = f'阶段{keyword.process_level-1}匹配规则'
                temp_last_level_rule,temp_last_level_rule_tag = get_last_level_rule(self.rules,keyword.process_level,local_matched_rule,target_file_name,target_sheet_name,keyword)
                if keyword.matched_info.get(last_matched_rule_col_name) == temp_last_level_rule:
                    matched_tag = True
                    break
        if matched_tag:
            local_matched_info = deepcopy(keyword.matched_info)
            local_matched_info.update({'rule_tag':temp_last_level_rule_tag})
            return create_classified_keyword(
                new_keyword=new_keyword,
                keyword=keyword,
                matched_rule=matched_rule,
                process_level=keyword.process_level,
                target_file_name=target_file_name,
                target_sheet_name=target_sheet_name,
                matched_info=local_matched_info
                
            )
        if keyword.process_level == 2:    
            return create_classified_keyword(
                new_keyword=new_keyword,
                keyword=keyword,
                matched_rule='',
                process_level=keyword.process_level,
                target_file_name=keyword.source_file_name,
                target_sheet_name='未匹配',
                matched_info=keyword.matched_info
            )
        else:
            return create_classified_keyword(
                new_keyword=new_keyword,
                keyword=keyword,
                matched_rule='',
                process_level=keyword.process_level,
                target_file_name=keyword.source_file_name,
                target_sheet_name=keyword.source_sheet_name,
                matched_info=keyword.matched_info
            )
          
                
            
