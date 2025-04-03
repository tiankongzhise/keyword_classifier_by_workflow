
from typing import List,TYPE_CHECKING,Dict,Literal,overload,Tuple,cast,Union
from pathlib import Path

from . import models


import pandas as pd
import datetime

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
                temp_dict[classified_keyword.rule_tag_column] = classified_keyword.rule_tag
                temp_dict[classified_keyword.level_rule_column] = classified_keyword.matched_rule
            if classified_keyword.level >3:
                temp_dict[classified_keyword.parent_rule_column] = classified_keyword.parent_rule
                
            result.append(temp_dict)
        return pd.DataFrame(result)
                
            
        
  
    def transform_to_df(self,data:List[models.ClassifiedKeyword]|List[models.UnMatchedKeyword])->pd.DataFrame:
        map_func = {
            models.UnMatchedKeyword:self.transfrom_unmathced_keywords,
            models.ClassifiedKeyword:self.transfrom_classified_keywords
        }
        return map_func[type(data[0])](data)
        
    def fromat_matched_rule_dict(self,rule_item:models.WorkFlowRule)->Dict:
        result = {
        'level': rule_item.level,
        'output_name' : rule_item.output_name,
        'matched_rule': rule_item.rule
        }
        if result['level'] >= 2:
            result['classified_sheet_name'] = rule_item.classified_sheet_name
        if result['level'] >= 3:
             result['rule_tag'] = rule_item.rule_tag
             result['rule_tag_column'] = f'阶段{result['level']}规则标签'
             result['level_rule_column'] = f'阶段{result['level']}匹配规则'
             
        if result['level'] >= 4:
            result['parent_rule'] = rule_item.parent_rule
            result['parent_rule_column'] = f'阶段{result['level']-1}匹配规则'
        return result
    def format_unmatched_keyword_dict(self,keyword_item:models.ClassifiedWord)->Dict:
        result = {
            'level':keyword_item.level,
            'keyword':keyword_item.keyword,
            'output_name':'未分类关键词' if keyword_item.level ==1 else keyword_item.source_file_name,
            'classified_sheet_name':'Sheet1' if keyword_item.source_file_name is None else '未匹配关键词' ,
            'source_sheet_name':keyword_item.source_sheet_name or 'Sheet1'
        }
        return result
    
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
                        msg = f'trans_words_to_cassified_result匹配异常，关键词匹配了一个分类工作流中不存在的规则。temp:{temp},workflow_rules:{workflow_rules}'
                        if self.error_callback:
                            self.error_callback(msg)
                        raise Exception(msg)

                    #创建映射字典
                    temp_dict = self.fromat_matched_rule_dict(rule_item.rules[0])
                    temp_dict['keyword'] = keyword
                    if temp_dict.get('output_name','') == '全':
                        temp_dict['output_name'] = temp.source_file_name
                    if temp_dict.get('classified_sheet_name','') == '全':
                        temp_dict['classified_sheet_name'] = temp.source_sheet_name
                    classified_keywords.append(
                        models.ClassifiedKeyword(**temp_dict))
                else:
                    # 创建映射关系
                    temp_dict = self.format_unmatched_keyword_dict(temp)
                    unclassified_keywords.append(
                            models.UnMatchedKeyword( **temp_dict))

            return models.ClassifiedResult(classified_keywords=classified_keywords,unclassified_keywords=unclassified_keywords)
        except Exception as e:
            msg = f"分类结果转换出错: {e},\nworkflow_rules: {workflow_rules},\nclassified_keywords:{classified_keywords},\nunclassified_keywords:{unclassified_keywords}"
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg) from e
    @staticmethod
    def generate_timestamped_path(
        base_dir: str | Path,
        filename: str,
        timestamp_format: str = "%Y%m%d%H%M%S",
        separator:str = '_',
        ext:str = ".xlsx"
    ) -> Path:
        """生成带时间戳的文件路径（自动处理路径拼接和时间戳插入）
        
        Args:
            base_dir: 基础目录路径
            filename: 原始文件名（不带时间戳）
            timestamp_format: 时间格式，默认 `%Y%m%d%H%M%S` (e.g. `20231004153045`)
            separator: 文件名与时间戳的分隔符，默认 `_`
            ext: 文件扩展名，默认 `.xlsx`
        
        Returns:
            Path: 完整路径，如 `/base_dir/filename_20231004153045.xlsx`
        """
        if isinstance(base_dir, str):
            base_dir = Path(base_dir)
        output_file:Path = base_dir / f'{filename}{separator}{datetime.datetime.now().strftime(timestamp_format)}.{ext}'
        return output_file
    def get_unclassified_keywords_from_result_df(self,result_df:pd.DataFrame,
                                                 source_file_name:str,
                                                 source_sheet_name:str,
                                                 level:int,
                                                 parent_workflow_rule_column_name:str|None = None,
                                                 parent_workflow_rule_str:str|None=None)->models.UnclassifiedKeywords|None:
        if result_df.empty:
            return None
        if '关键词' not in result_df.columns:
            msg = f'get_keywords_from_result_df: result_df中不存在关键词列，请检查数据格式。result_df:{result_df}'
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg)
        keywords = cast(List[str],result_df['关键词'].astype(str).tolist())
        
        return models.UnclassifiedKeywords(data=keywords,
                                           source_file_name=source_file_name,
                                           source_sheet_name=source_sheet_name,
                                           level=level,
                                           parent_workflow_rule_column_name=parent_workflow_rule_column_name,
                                           parent_workflow_rule_str=parent_workflow_rule_str,
                                           error_callback=self.error_callback)
    @staticmethod
    def create_fail_process_return_result(level:int,info:str) -> models.ProcessReturnResult:
        return models.ProcessReturnResult(
                level=level,
                status='fail',
                info=info,
                process_sheet_count=1,
                sheet_status_counts={"success":0,'fail':1,'warning':0},
                fail_items=[],
                warning_items=[],
                no_process_items=[],
            )
    @staticmethod
    def create_temp_process_return_result_dict(level:int):
            return {
            'level':level,
            'process_sheet_count':0,
            'sheet_status_counts':{'success':0, 'fail':0, 'warning':0,'no_process':0},
            'fail_items':[],
            'warning_items':[],
            'no_process_items':[]
        }
    @staticmethod
    def update_temp_process_return_result_dict(temp_dict,status:Literal['success','fail','warning','no_process'],info:str,output_file_name:str,output_sheet_name:str)->None:
        if status=='success':
            temp_dict['sheet_status_counts']['success']+=1
        elif status=='fail':
            temp_dict['sheet_status_counts']['fail']+=1
            temp_dict['fail_items'].append({
                'info':info,
                'output_file_name':output_file_name,
                'output_sheet_name':output_sheet_name
            })
        elif status=='warning':
            temp_dict['sheet_status_counts']['warning']+=1
            temp_dict['warning_items'].append({
                'info':info,
                'output_file_name':output_file_name,
                'output_sheet_name':output_sheet_name
            })
        elif status=='no_process':
            temp_dict['sheet_status_counts']['no_process']+=1
            temp_dict['no_process_items'].append({
                'info':info,
                'output_file_name':output_file_name,
                'output_sheet_name':output_sheet_name
            })
        else:
            raise ValueError(f'status must be success,fail,warning,but got {status}')
        temp_dict['process_sheet_count']+=1
        success_count = temp_dict['sheet_status_counts']['success'] + temp_dict['sheet_status_counts']['no_process']
        if success_count == temp_dict['process_sheet_count']:
            temp_dict['status'] = 'success'
        elif success_count == 0:
            temp_dict['status'] = 'fail'
        else:
            temp_dict['status'] = 'some_fail'
        temp_dict['info'] = '...'
    
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['output_name'],keyword_status:Literal['match'])->Dict[str,List[models.ClassifiedKeyword]]:
        ...
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['output_name'],keyword_status:Literal['unmatch'])->Dict[str,List[models.UnMatchedKeyword]]:
        ...
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['sheet'],keyword_status:Literal['match'])->Dict[Tuple[str,str],List[models.ClassifiedKeyword]]:
        ...
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['sheet'],keyword_status:Literal['unmatch'])->Dict[Tuple[str,str],List[models.UnMatchedKeyword]]:
        ...
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['parent_rule'],keyword_status:Literal['match'])->Dict[Tuple[str,str,str],List[models.ClassifiedKeyword]]:
        ...
    @overload
    @staticmethod
    def get_classification_groups(v:models.ClassifiedResult,mode:Literal['parent_rule'],keyword_status:Literal['unmatch'])->Dict[Tuple[str,str,str],List[models.UnMatchedKeyword]]:
        ...
    @staticmethod
    def get_classification_groups(
        v: models.ClassifiedResult,
        mode: Literal["output_name", "sheet", "parent_rule"],
        keyword_status: Literal["match", "unmatch"]
    ) -> Union[
        Dict[str, List[models.ClassifiedKeyword]],
        Dict[str, List[models.UnMatchedKeyword]],
        Dict[Tuple[str, str], List[models.ClassifiedKeyword]],
        Dict[Tuple[str, str], List[models.UnMatchedKeyword]],
        Dict[Tuple[str, str, str], List[models.ClassifiedKeyword]],
        Dict[Tuple[str, str, str], List[models.UnMatchedKeyword]],
    ]:
        if mode == 'output_name':
            if keyword_status == 'match':
                return cast(Dict[str,List[models.ClassifiedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            elif keyword_status == 'unmatch':
                return cast(Dict[str,List[models.UnMatchedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            else:
                raise ValueError(f'get_classification_groups的keyword_status异常,值应该为Literal["match","unmatch"],实际为{keyword_status}')
        elif mode == 'sheet':
            if keyword_status == 'match':
                return cast(Dict[Tuple[str,str],List[models.ClassifiedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            elif keyword_status == 'unmatch':
                return cast(Dict[Tuple[str,str],List[models.UnMatchedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            else:
                raise ValueError(f'get_classification_groups的keyword_status异常,值应该为Literal["match","unmatch"],实际为{keyword_status}')
        elif mode == 'parent_rule':
            if keyword_status == 'match':
                return cast(Dict[Tuple[str,str,str],List[models.ClassifiedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            elif keyword_status == 'unmatch':
                return cast(Dict[Tuple[str,str,str],List[models.UnMatchedKeyword]],v.get_grouped_keywords(mode,keyword_status))
            else:
                raise ValueError(f'get_classification_groups的keyword_status异常,值应该为Literal["match","unmatch"],实际为{keyword_status}')
        else:
            raise ValueError(f"get_classification_groups的mode异常,值应该为Literal['output_name','sheet','parent_rule'],实际为{mode}")
            
            
    # def add_sheet_columns(
    #     self,
    #     df_old: pd.DataFrame,
    #     new_df: pd.DataFrame,
    #     key_mapping: dict,  # 格式: {"原表列名": "新表列名"}
    #     tag_mapping: dict,  # 格式: {"新表标签列名": "原表输出列名"}
    #     keep_unmatched: bool = True  # 是否保留未匹配的行
    # ) -> pd.DataFrame:
    #     """
    #     支持列名映射的数据合并工具
        
    #     Args:
    #         excel_path: 原Excel路径
    #         sheet_name: 目标Sheet名称
    #         new_df: 包含标签的新DataFrame
    #         key_mapping: 新旧表匹配列映射 {"原表列": "新表列"}
    #         tag_mapping: 标签列映射 {"新表列": "原表输出列"}
    #         new_file_path: 新文件路径，如果为None则覆盖原文件
    #         keep_unmatched: 是否保留原表未匹配的行
    #         wildcard: 通配符值（默认"全"）
    #     """        
    #     # 1. 列名检查
    #     missing_old_keys = [k for k in key_mapping if k not in df_old.columns]
    #     missing_new_keys = [v for v in key_mapping.values() if v not in new_df.columns]
    #     missing_tags = [k for k in tag_mapping if k not in new_df.columns]
        
    #     if missing_old_keys:
    #         raise ValueError(f"原表缺少匹配列: {missing_old_keys}")
    #     if missing_new_keys:
    #         raise ValueError(f"新表缺少匹配列: {missing_new_keys}") 
    #     if missing_tags:
    #         raise ValueError(f"新表缺少标签列: {missing_tags}")

    #     # 2. 统一列名（临时修改新DF列名以匹配原表）
    #     new_df_renamed = new_df.rename(
    #         columns={v: k for k, v in key_mapping.items()}
    #     )
        
    #     # 3. 合并数据
    #     how = 'left' if keep_unmatched else 'inner'
    #     merged = pd.merge(
    #         left=df_old,
    #         right=new_df_renamed[list(key_mapping.keys()) + list(tag_mapping.keys())],
    #         on=list(key_mapping.keys()),  # 使用统一后的列名匹配
    #         how=how
    #     )
        
    #     # 4. 重命名标签列
    #     merged.rename(columns=tag_mapping, inplace=True)
        
    #     return merged
        
        
    def add_sheet_columns(
        self,
        df_old: pd.DataFrame,
        new_df: pd.DataFrame,
        key_mapping: dict,  # {"原表列名": "新表列名"}
        tag_mapping: dict,  # {"新表标签列名": "原表输出列名"}
        keep_unmatched: bool = True
    ) -> pd.DataFrame:
        """
        支持列名映射的数据合并工具（修复列名重复问题）
        """
        # 1. 列名检查
        missing_old_keys = [k for k in key_mapping if k not in df_old.columns]
        missing_new_keys = [v for v in key_mapping.values() if v not in new_df.columns]
        missing_tags = [k for k in tag_mapping if k not in new_df.columns]
        
        if missing_old_keys:
            raise ValueError(f"原表缺少匹配列: {missing_old_keys}")
        if missing_new_keys:
            raise ValueError(f"新表缺少匹配列: {missing_new_keys}") 
        if missing_tags:
            raise ValueError(f"新表缺少标签列: {missing_tags}")

        # 2. 临时重命名新表的匹配列（避免合并时冲突）
        # 例如：将新表的 "阶段3匹配规则" 改为 "阶段3匹配规则_new"
        temp_suffix = "_new"  # 临时后缀
        new_rename_map = {
            v: f"{k}{temp_suffix}"  # 原列名 -> 临时列名（加后缀）
            for k, v in key_mapping.items()
        }
        new_df_renamed = new_df.rename(columns=new_rename_map)
        
        # 3. 合并数据（使用临时列名作为 on 参数）
        how = 'left' if keep_unmatched else 'inner'
        merged = pd.merge(
            left=df_old,
            right=new_df_renamed[
                [f"{k}{temp_suffix}" for k in key_mapping.keys()] +  # 临时匹配列
                list(tag_mapping.keys())                              # 标签列
            ],
            left_on=list(key_mapping.keys()),     # 原表列名
            right_on=[f"{k}{temp_suffix}" for k in key_mapping.keys()],  # 新表临时列名
            how=how
        )
        
        # 4. 删除临时列（可选）
        merged.drop(
            columns=[f"{k}{temp_suffix}" for k in key_mapping.keys()],
            inplace=True
        )
        
        # 5. 重命名标签列
        merged.rename(columns=tag_mapping, inplace=True)
        
        return merged

    def get_sheet_match_df(self,df_list:List[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(df_list,ignore_index=True)
    def get_tag_column_name(self,level:int)->str:
        return f'阶段{level}规则标签'
    def get_level_column_name(self,level:int)->str:
        return f'阶段{level}匹配规则'
