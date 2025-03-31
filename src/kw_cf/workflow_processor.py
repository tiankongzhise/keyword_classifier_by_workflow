from pathlib import Path

from .keyword_classifier import KeywordClassifier
from .excel_handler import ExcelHandler
from .logger_config import logger
from typing import List,Dict,TypedDict,Optional,Callable,cast
from . import models
import pandas as pd
import datetime


class StageOneRestsultTypeDict(TypedDict):
    code:str
    level:int
    next_stage:int
    file_path:Dict[str,Path]
    message:str

    
class StageTwoRestsultTypeDict(TypedDict):
    ...
class StageThreeRestsultTypeDict(TypedDict):
    ...
class StageHighResultTypeDict(TypedDict):
    ...
class Stage2OutputNameDict(TypedDict):
    file_path:str
    classified_sheet_name:List[str]
    
    

class WorkFlowProcessor:
    def __init__(self,
                 excel_handler: ExcelHandler | None = None,
                 keyword_classifier: KeywordClassifier | None = None,
                 error_callback: Optional[Callable] = None
                 ):
        """初始化工作流处理器
        
        Args:
            classifier: 关键词分类器实例，如果为None则创建新实例
            excel_handler: Excel处理器实例，如果为None则创建新实例
        """
        self.excel_handler:ExcelHandler = excel_handler or ExcelHandler(error_callback)
        self.classifier:KeywordClassifier = keyword_classifier or KeywordClassifier(error_callback=error_callback)
        self.error_callback:Optional[Callable] = error_callback
        self.workflow_rules:Optional[models.WorkFlowRules] = None
        self.process_result_file:Optional[Dict[str,pd.DataFrame]] = None
        self.process_result_classified_file:Optional[Dict[str,Dict[str,List[str]|str]]] = None

        self.output_dir = Path('./工作流结果')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    
    def _transfrom_unmathced_keywords(self,unmatched_keywords:List[models.UnMatchedKeyword])->pd.DataFrame:
        result = []
        for unmatched_keyword in unmatched_keywords:
            temp_dict = {}
            temp_dict['关键词'] = unmatched_keyword.keyword
            temp_dict['分类层级'] = unmatched_keyword.level
            if unmatched_keyword.parent_rule:
                temp_dict['父级规则'] = unmatched_keyword.parent_rule
            result.append(temp_dict)
        return pd.DataFrame(result)
    
    def _transfrom_classified_keywords(self,classified_keywords:List[models.ClassifiedKeyword])->pd.DataFrame:
        result = []
        for classified_keyword in classified_keywords:
            temp_dict = {}
            temp_dict['关键词'] = classified_keyword.keyword
            temp_dict['匹配的规则'] = classified_keyword.matched_rule
            if classified_keyword.parent_rule:
                temp_dict['父级规则'] = classified_keyword.parent_rule
            result.append(temp_dict)
        return pd.DataFrame(result)
                
            
        

    def _transform_to_df(self,data:List[models.UnMatchedKeyword|models.ClassifiedKeyword])->pd.DataFrame:
        map_func = {
            models.UnMatchedKeyword:self._transfrom_unmathced_keywords,
            models.ClassifiedKeyword:self._transfrom_classified_keywords
        }
        return map_func[type(data[0])](data)
        
        
    def _trans_words_to_cassified_result(self,classify_result:List[models.ClassifiedWord],mapping_dict:dict)->Optional[models.ClassifiedResult]:
        classified_keywords = []
        unclassified_keywords = []
        try:
            # 处理分类结果
            for temp in classify_result:
                keyword = temp.keyword
                matched_rules = temp.matched_rule
                temp_dict = {}
                if matched_rules:
                    temp_dict = {
                        'level':mapping_dict['level'],
                        'keyword':keyword,
                        'matched_rule':matched_rules,
                        'output_name':mapping_dict[matched_rules].get('output_name'),
                        'classified_sheet_name':mapping_dict[matched_rules].get('classified_sheet_name'),
                        'parent_rule':mapping_dict[matched_rules].get('parent_rule')
                    }
                    classified_keywords.append(
                        models.ClassifiedKeyword(**temp_dict))
                else:
                    if mapping_dict.get('level') == 1:
                        temp_dict = {
                            'level':mapping_dict['level'],
                            'keyword':keyword,
                            'output_name':'未匹配关键词',
                            'classified_sheet_name':'Sheet1'
                        }

                    else:
                        output_name = list(mapping_dict.values())[1].get('output_name')
                        for key,value in mapping_dict.items():
                            if isinstance(value,dict):
                                if output_name != value.get('output_name'):
                                    msg = f'异常情况，传入的隐射关系存在多个来源文件夹,请检查规则映射关系{mapping_dict},output_name:{output_name},value:{value.get("output_name")}'
                                    raise Exception(msg)
                        temp_dict = {
                            'level':mapping_dict['level'],
                            'keyword':keyword,
                            'output_name':output_name,
                            'classified_sheet_name':'未匹配关键词'
                        }
                    unclassified_keywords.append(
                            models.UnMatchedKeyword( **temp_dict))
            if classified_keywords:
                return models.ClassifiedResult(classified_keywords=classified_keywords,unclassified_keywords=unclassified_keywords)
        except Exception as e:
            msg = f"分类结果转换出错: {e},\nmapping_dict: {mapping_dict},\nclassified_keywords:{classified_keywords},\nunclassified_keywords:{unclassified_keywords}"
            raise Exception(msg)
    def _create_mapping_dict(self,workflow_rules:models.WorkFlowRules,level:int)->dict:
        mapping_dict = {}
        mapping_dict['level'] = level
        for rule in workflow_rules.rules:
            mapping_dict[rule.rule] = {
                'output_name':rule.output_name,
                'classified_sheet_name':rule.classified_sheet_name,
                'parent_rule':rule.parent_rule
            }
        return mapping_dict

    def _get_classified_results(self,unclassified_keywords:models.UnclassifiedKeywords,workflow_rules:models.WorkFlowRules,level:int,
                            error_callback=None)->Optional[models.ClassifiedResult]:
        """关键词分类
        
        Args:
            keywords: 未分类关键词
            workflow_rules: 工作流规则
            error_callback: 错误回调函数
        
        Returns:
            ClassifiedResult:分类结果
        """
        # 创建规则与输出映射字典
        mapping_dict = self._create_mapping_dict(workflow_rules,level)
        
        # 获取分类规则列表，方便后续处理
        rules = workflow_rules.to_rules_list()
        
        # 设置分类规则
        self.classifier.set_rules(models.SourceRules(data=rules,error_callback=error_callback))
        
        # 分类关键词
        classify_result = self.classifier.classify_keywords(unclassified_keywords)
        
        # 转换分类结果
        classified_reuslt =  self._trans_words_to_cassified_result(classify_result,mapping_dict)
        
        return classified_reuslt
        
    def _process_stage_df(self,pipeline_data:Dict[str,pd.DataFrame],level:int,**kwargs)->models.UnclassifiedKeywords:
        mask = None
        try:
            error_callback = kwargs.get('error_callback')
            if level == 2:
                if 'Sheet1' not in pipeline_data.keys():
                    msg = "第一阶段关键词分类结果中未找到Sheet1列，请检查是否正确"
                    if error_callback:
                        error_callback(msg)
                    raise Exception(msg)
                
                return models.UnclassifiedKeywords(data=cast(List[str], pipeline_data['Sheet1']['关键词'].astype(str).tolist()),error_callback=error_callback) #noqa
            elif level == 3:
                if kwargs is None or kwargs.get('classified_sheet_name') is None:
                    msg = '第三阶段关键词分类，_process_stage_df未传入必要的classified_sheet_name参数'
                    if error_callback:
                        error_callback(msg)
                    raise Exception(msg)
                return models.UnclassifiedKeywords(data=cast(List[str],pipeline_data[kwargs['classified_sheet_name']]['关键词'].astype(str).tolist()),error_callback=error_callback)
            elif level >3:
                # 检查 level > 3 时是否传入了必要参数
                required_args = ["classified_sheet_name", "parent_rule"]
                missing_args = [arg for arg in required_args if arg not in kwargs]
                if missing_args:
                    raise ValueError(
                        f"当 level > 3 时，必须传入以下参数: {', '.join(missing_args)}"
                    )
                parent_rule_columon_name = '阶段'+str(level-1)
                if parent_rule_columon_name not in pipeline_data[kwargs['classified_sheet_name']].columns:
                    err_msg = f'classified_sheet_name:{kwargs['classified_sheet_name']},parent_rule_columon_name:{parent_rule_columon_name}不存在，无法进行匹配，pipeline_data[kwargs["classified_sheet_name"]].columns:{pipeline_data[kwargs["classified_sheet_name"]].columns}'
                    if error_callback:
                        error_callback(err_msg)
                    logger.error(err_msg)
                    raise Exception(err_msg)
                logger.debug(f'pipeline_data[kwargs["classified_sheet_name"]]:{pipeline_data[kwargs["classified_sheet_name"]]}')
                logger.debug(f'kwargs["parent_rule"]:{kwargs["parent_rule"]}')
                logger.debug(f'pipeline_data[kwargs["classified_sheet_name"]][parent_rule_columon_name]:{pipeline_data[kwargs["classified_sheet_name"]][parent_rule_columon_name]}')
                logger.debug(f'set(kwargs["parent_rule"]):{set(kwargs["parent_rule"])}')
                if isinstance(kwargs['parent_rule'],str):
                    match_parent_rule:List[str] = [kwargs['parent_rule']]
                elif isinstance(kwargs['parent_rule'],list):
                    match_parent_rule:List[str] = kwargs['parent_rule']
                else:
                    raise Exception(f'parent_rule:{kwargs["parent_rule"]}类型错误')


                mask =  pipeline_data[kwargs['classified_sheet_name']][parent_rule_columon_name].isin(list(set(match_parent_rule)))
                filtered_df  = pipeline_data[kwargs['classified_sheet_name']][mask].copy()
                logger.debug(f'filtered_df:{filtered_df}')
                if filtered_df.empty:
                    return models.UnclassifiedKeywords(data=[],error_callback=self.error_callback)
                return models.UnclassifiedKeywords(data=cast(List[str],filtered_df['关键词'].astype(str).tolist()),error_callback=self.error_callback)
            else:
                msg = f'第{level}尚未实现相关功能！'
                if error_callback:
                    error_callback(msg)
                raise Exception(msg)            
        except Exception as e:
            if mask is not None:
                logger.debug(f'err_mask:{mask}')
            msg = f"处理阶段性分词结果到待分类关键词：{e}"
            if kwargs.get('error_callback'):
                kwargs['error_callback'](msg)
            raise Exception(msg)
        

    def _special_rules_match_process(self,workflow_rules:models.WorkFlowRules,stage_results:Dict,
                                      error_callback=None)->models.WorkFlowRules:
        try:
            output_name_list = []
            classified_sheet_name_dict = {}
            for key,value in stage_results.items():
                output_name_list.append(key)
                if classified_sheet_name_dict.get(key) is None:
                    classified_sheet_name_dict[key] = value.get('classified_sheet_name')
                else:
                    classified_sheet_name_dict[key].extend(value.get('classified_sheet_name'))
            special_output_name_rules = workflow_rules.filter_rules(output_name = '全')
            temp_list = []
            if special_output_name_rules:
                for rule in special_output_name_rules.rules:
                    for output_name in output_name_list:
                        temp_list.append(rule.model_copy(update={'output_name':output_name}))
                special_output_name_rules = models.WorkFlowRules(rules=temp_list)
            temp_list = []
            if special_output_name_rules:
                temp_rules = special_output_name_rules
            else:
                temp_rules = workflow_rules
            
            special_classified_sheet_name_rules = temp_rules.filter_rules(classified_sheet_name = "全")
            if special_classified_sheet_name_rules:
                for rule in special_classified_sheet_name_rules.rules:
                    for classified_sheet_name in classified_sheet_name_dict[rule.output_name]:
                        temp_list.append(rule.model_copy(update={'classified_sheet_name':classified_sheet_name}))
                special_classified_sheet_name_rules = models.WorkFlowRules(rules=temp_list)
            return special_classified_sheet_name_rules
        except Exception as e:
            msg = f'将"全"翻译为全部匹配元素时出错，str({e})'
            if error_callback:
                error_callback(msg)
            raise Exception(msg)
    def _special_rules_match_process_v1(self,workflow_rules:models.WorkFlowRules,stage_results:Dict)->models.WorkFlowRules:
        try:
            output_name_list = []
            classified_sheet_name_dict = {}
            for key,value in stage_results.items():
                output_name_list.append(key)
                classified_sheet_name_list:List = value.get('classified_sheet_name')
                if len(classified_sheet_name_list) > 1:
                    classified_sheet_name_list.remove('Sheet1')
                if classified_sheet_name_dict.get(key) is None:
                    classified_sheet_name_dict[key] = classified_sheet_name_list
                else:
                    classified_sheet_name_dict[key].extend(classified_sheet_name_list)
            special_output_name_rules = workflow_rules.filter_rules(output_name = '全')
            temp_list = []
            if special_output_name_rules:
                for rule in special_output_name_rules.rules:
                    for output_name in output_name_list:
                        temp_list.append(rule.model_copy(update={'output_name':output_name}))
                special_output_name_rules = models.WorkFlowRules(rules=temp_list)
            temp_list = []
            if special_output_name_rules:
                temp_rules = special_output_name_rules
            else:
                temp_rules = workflow_rules
            
            special_classified_sheet_name_rules = temp_rules.filter_rules(classified_sheet_name = "全")
            if special_classified_sheet_name_rules:
                for rule in special_classified_sheet_name_rules.rules:
                    for classified_sheet_name in classified_sheet_name_dict[rule.output_name]:
                        temp_list.append(rule.model_copy(update={'classified_sheet_name':classified_sheet_name}))
                special_classified_sheet_name_rules = models.WorkFlowRules(rules=temp_list)
            return special_classified_sheet_name_rules
        except Exception as e:
            msg = f'将"全"翻译为全部匹配元素时出错，str({e})'
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg)
     
    def add_matched_rule_with_pandas(
        self,
        excel_path: str,
        sheet_name: str,
        keyword_to_rule:Dict[str,str],
        new_column_name: str = "规则1"
    ):
        """
        使用 pandas 匹配 keyword 并新增列
        
        Args:
            data: 你的数据结构
            excel_path: Excel 文件路径
            sheet_name: 要操作的 sheet 名称
            new_column_name: 新增列的名称
        """
        try:
            # 读取原 Excel 文件
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            

            # 新增列，默认值为空（未匹配到的行留空）
            df[new_column_name] = df["关键词"].map(keyword_to_rule)
            
            # 保存回原文件
            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)   
            return True
        except Exception as e:
            err_msg = f'add_matched_rule_with_pandas 保存文件失败{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise err_msg
    def add_matched_rules_with_pandas(
        self,
        excel_path: str,
        sheet_name: str,
        column_mappings: Dict[str, Dict[str, str]],
        keyword_column: str = "关键词"
    ) -> bool:
        """
        使用 pandas 匹配 keyword 并新增多列规则，每列可以有完全独立的列名
        
        Args:
            excel_path: Excel 文件路径
            sheet_name: 要操作的 sheet 名称
            column_mappings: 字典，键是新列名，值是该列的关键词到规则的映射
                           例如: {
                               "阶段X分类规则": {"苹果": "水果", "香蕉": "水果"},
                               "阶段X分类标签": {"苹果": "红色", "香蕉": "黄色"},
                               "形状分类": {"苹果": "圆形", "香蕉": "长形"}
                           }
            keyword_column: 用于匹配的关键词列名，默认为"关键词"
                             
        Returns:
            bool: 操作是否成功
            
        Raises:
            Exception: 操作失败时抛出异常
        """
        try:
            # 读取原 Excel 文件
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            
            # 检查关键词列是否存在
            if keyword_column not in df.columns:
                raise ValueError(f"数据中不存在指定的关键词列: {keyword_column}")
            
            # 为每个列映射添加新列
            for new_column, rule_map in column_mappings.items():
                df[new_column] = df[keyword_column].map(rule_map)
            
            # 保存回原文件
            with pd.ExcelWriter(
                excel_path,
                engine='openpyxl',
                mode='a',
                if_sheet_exists='replace'
            ) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
            return True
        except Exception as e:
            err_msg = f'add_matched_rules_with_pandas 保存文件失败: {str(e)}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(err_msg)      
            
    def get_level_rules(self,workflow_rules:models.WorkFlowRules,stage_results:Dict,
                                      error_callback=None)->models.WorkFlowRules:
        
        nom_rules = workflow_rules.filter_rules(output_name = lambda x:x !='全',classified_sheet_name = lambda x:x!='全')
        special_rules = self._special_rules_match_process(workflow_rules,stage_results,error_callback)
        temp_list = []
        if nom_rules:
            temp_list.extend(nom_rules.rules)
        if special_rules:
            temp_list.extend(special_rules.rules)
        return models.WorkFlowRules(rules=temp_list)
    
    def get_level_rules_v1(self,workflow_rules:models.WorkFlowRules,stage_results:Dict)->models.WorkFlowRules:
        nom_rules = workflow_rules.filter_rules(output_name = lambda x:x !='全',classified_sheet_name = lambda x:x!='全')
        special_rules = self._special_rules_match_process_v1(workflow_rules,stage_results)
        temp_list = []
        if nom_rules:
            temp_list.extend(nom_rules.rules)
        if special_rules:
            temp_list.extend(special_rules.rules)
        return models.WorkFlowRules(rules=temp_list)
    
    def process_stage1(self,keywords:models.UnclassifiedKeywords,workflow_rules:models.WorkFlowRules,
                       error_callback=None)->models.ClassifiedResult:
        """处理第一阶段的关键词分类"""
        try:
            # 获取一阶段分类规则
            stage1_rules = workflow_rules.get_rules_by_level(1)
            if stage1_rules is None:
                msg = '第一阶段关键词分类规则为空'
                if error_callback:
                    error_callback(msg)
                raise Exception(msg)
            # 分类关键词
            result = self._get_classified_results(keywords,stage1_rules,1,error_callback = error_callback)
            if result is None:
                msg = '第一阶段关键词分类结果为空'
                if error_callback:
                    error_callback(msg)
                raise Exception(msg)
            return result
        except Exception as e:
            msg = f"获取一阶段分类规则失败：{e}"
            if error_callback:
                error_callback(f"获取一阶段分类规则失败：{e}")
            raise Exception(msg) from e

    def save_stage1_results(self,classified_result:models.ClassifiedResult,error_callback=None)->StageOneRestsultTypeDict:
        """保存第一阶段分类结果
        
        Args:
            classified_result: 分类结果
            error_callback: 错误回调函数
            
        Returns:
            保存的文件路径字典
        """
        # result = {
        #     code:None,
        # }
        success_file_paths:Dict[str,Path] = {}
        try:
            # 获取分类结果
            unmatched_keywords = classified_result.get_grouped_keywords(group_by='output_name',match_type='unmatch')
            matched_keywords = classified_result.get_grouped_keywords(group_by='output_name',match_type='match')
            
            try:
                # 保存分类失败的关键词
                if unmatched_keywords:
                    for output_name, unclassify_keyword_list in unmatched_keywords.items():
                        output_file = self.output_dir / f'{output_name}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                        df = self._transform_to_df(unclassify_keyword_list)
                        self.excel_handler.save_results(df, output_file,sheet_name='Sheet1')
            except Exception as e:
                err_msg = f'保存分类失败的关键词失败：{e}'
                if error_callback:
                    error_callback(err_msg)
                raise Exception(f"保存分类失败的关键词失败：{e}")
            
            try:
                if matched_keywords:
                    for output_name, matched_keyword_list in matched_keywords.items():
                        output_file:Path = self.output_dir / f'{output_name}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                        df = self._transform_to_df(matched_keyword_list)
                        self.excel_handler.save_results(df, output_file,sheet_name='Sheet1')
                        success_file_paths[cast(str,output_name)] = output_file
                    return success_file_paths
            except Exception as e:
                err_msg = f'保存分类成功的关键词失败：{e}'
                if error_callback:
                    error_callback(err_msg)
                raise Exception(f"保存分类成功的关键词失败：{e}")
        except Exception as e:
            err_msg = f'获取分类结果失败：{e}'
            if error_callback:
                error_callback(err_msg)
            raise Exception(f"获取分类结果失败：{e}")
    

    def process_stage2(self, stage1_files: Dict[str, Path], workflow_rules: models.WorkFlowRules, 
                      error_callback=None) -> Dict[str, models.ClassifiedResult]:
        """处理阶段2：分层处理（Sheet2处理）
        
        Args:
            stage1_files: 阶段1生成的文件路径字典
            workflow_rules: 工作流规则字典
            error_callback: 错误回调函数
            
        Returns:
            更新后的文件路径字典
        """
        # 检查是否有Sheet2规则
        if workflow_rules.get('Sheet2') is None:
            msg = '找不到Sheet2规则，已经返回'
            if error_callback:
                error_callback(msg)
            return msg
        
        try:
            # 获取分类流程2的规则
            sheet2_rules = workflow_rules.filter_rules(source_sheet_name='Sheet2')
            stage2_results = {}
            
            # 处理每个阶段1文件
            for output_name, file_path in stage1_files.items():
                # 读取阶段1文件
                stage1_df = self.excel_handler.read_stage_results(file_path)
                
                #获取需要分类的关键词
                unclassified_keyword = self._process_stage_df(stage1_df,2,error_callback=error_callback)
                
                # 获取分类规则
                output_name_rules = sheet2_rules.filter_rules(output_name=output_name)
                
                if output_name_rules:
                    classified_result = self._get_classified_results(unclassified_keyword,output_name_rules,2,error_callback=error_callback)
                    stage2_results[output_name] = classified_result
                else:
                    msg = f'找不到{output_name}的Sheet2规则，已经返回'
                    if error_callback:
                        error_callback(msg)
            return stage2_results
        except Exception as e:
            raise Exception(f"处理阶段2失败: {str(e)}")
    

    def save_stage2_results(self, stage1_files,classified_result: Dict[str,models.ClassifiedResult], error_callback=None) -> dict[str, Stage2OutputNameDict]:
        """保存阶段2分类结果
        
        Args:
            classified_result: 分类结果
            error_callback: 错误回调函数
            
        Returns:
            保存的文件路径字典
        """
        try:
            stage2_result = {}
            for key,values in classified_result.items():
                file_path = stage1_files[key]
                # 使用Excel写入器追加新Sheet
                with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
                    stage2_result[key] = {'file_path': file_path, 'classified_sheet_name': []}
                    if values is None:
                        logger.warning(f'{key}没有分类结果')
                        continue
                    for key, classified_keyword_list in values.group_by_output_name_and_sheet(match_type='match').items():
                        output_name,classified_sheet_name = key
                        df = self._transform_to_df(classified_keyword_list)
                        df.to_excel(writer, sheet_name=classified_sheet_name, index=False)
                        stage2_result[output_name]['classified_sheet_name'].append(classified_sheet_name)
                    for key, unclassified_keyword_list in values.group_by_output_name_and_sheet(match_type='unmatch').items():
                        output_name,classified_sheet_name = key
                        df = self._transform_to_df(unclassified_keyword_list)
                        df.to_excel(writer, sheet_name=classified_sheet_name, index=False)
                        
                
            return stage2_result

        except Exception as e:
            err_msg = f'保存分类成功的关键词失败：{e}'
            if error_callback:
                error_callback(err_msg)
            raise Exception(f"保存分类成功的关键词失败：{e}")
    def process_stage3(self, stage2_results: Dict[str,Dict[str,list]], workflow_rules: models.WorkFlowRules, 
                      error_callback=None) -> Optional[Dict[str,Dict[str,models.ClassifiedResult]]]:
        """处理阶段3：分类后处理（Sheet3处理）
        
        Args:
            stage2_results: 阶段2分类结果
            workflow_rules: 工作流规则字典
            error_callback: 错误回调函数
            
        Returns:
            更新后的文件路径字典
        """
        # 检查是否有Sheet3规则
        if workflow_rules.get('Sheet3') is None:
            msg = '找不到Sheet3规则，已经返回'
            if error_callback:
                error_callback(msg)
            return None
        
        try:
            # 获取分类流程3的规则
            sheet3_rules = workflow_rules.filter_rules(source_sheet_name='Sheet3')
            sheet3_rules = self.get_level_rules(sheet3_rules,stage2_results,error_callback)
            stage3_results = {}
            
            # 处理每个阶段1文件
            for output_name, values in stage2_results.items():
                file_path = values['file_path']
                if values.get('classified_sheet_name') is None:
                    continue
                classified_sheet_name_list = values['classified_sheet_name']
                # 读取阶段2文件
                stage2_df = self.excel_handler.read_stage_results(file_path)
                
                for classified_sheet_name in classified_sheet_name_list:
                    
                    #获取需要分类的关键词
                    unclassified_keyword = self._process_stage_df(stage2_df,3,classified_sheet_name = classified_sheet_name,error_callback=error_callback)

                    # 获取分类规则

                    output_name_rules = sheet3_rules.filter_rules(output_name=output_name,classified_sheet_name=classified_sheet_name)

                    if output_name_rules:
                        classified_result = self._get_classified_results(unclassified_keyword,output_name_rules,3,error_callback=error_callback)
                        # if stage3_results == {}:
                        #     stage3_results[output_name] = {}
                        # elif stage3_results.get(output_name) is None:
                        #     stage3_results[output_name] = {}
                        # else:
                        #     stage3_results[output_name][classified_sheet_name] = classified_result
                        if output_name not in stage3_results:
                            stage3_results[output_name] = {}
                        stage3_results[output_name][classified_sheet_name] = classified_result
                    else:
                        msg = f'找不到{output_name}的Sheet2规则，已经返回'
                        if error_callback:
                            error_callback(msg)
            return stage3_results
        except Exception as e:
            raise Exception(f"处理阶段3失败: {str(e)}")
    
      
    def save_stage3_results(self, stage2_file:Dict[str,Stage2OutputNameDict],stage3_results:Optional[Dict[str,Dict[str,models.ClassifiedResult]]], error_callback=None) -> dict[str, Path]:
        """保存阶段3分类结果
        
        Args:
            classified_result: 分类结果
            error_callback: 错误回调函数
            
        Returns:
            保存的文件路径字典
        """
        if stage3_results is None:
            logger.warning('stage3返回了None,可能是没有Sheet3规则')
            return stage2_file

        try:
            for output_name,result_dict in stage3_results.items():
                if result_dict == {}:
                    continue
                file_path = stage2_file[output_name]['file_path']
                logger.debug(f'file_path:{file_path}')
                
                for classified_sheet_name,classified_result in result_dict.items():
                    if classified_result is None:
                        continue
                    # 构建 keyword 到 matched_rule 的映射
                    keyword_to_rule = {
                        kw.keyword: kw.matched_rule 
                        for kw in classified_result.filter(classified_conditions={'classified_sheet_name':classified_sheet_name}).classified_keywords
                    }
                    self.add_matched_rule_with_pandas(excel_path = file_path,
                                                      sheet_name = classified_sheet_name,
                                                      keyword_to_rule = keyword_to_rule,
                                                      new_column_name = '阶段3'
                                                      )
            return stage2_file
        except  Exception as e:
            err_msg = f'保存阶段三分类结果失败：{e}'
            if error_callback:
                error_callback(err_msg)
            raise Exception(err_msg)
 
    def process_stage_high(self,level:int)->Dict[str,Dict[str,models.ClassifiedResult]]|None:
        """处理阶段高阶段：工作流三阶段以上
        Args:
            level: 分类级别
        Returns:
            更新后的文件路径字典
        """
        # 检查是否有高阶段规则
        if self.workflow_rules.get(level) is None:
            msg = f'找不到{level}规则，已经返回'
            if self.error_callback:
                self.error_callback(msg)
            return None
        
        try:
            # 获取分类流程的规则
            level_rules = self.workflow_rules.filter_rules(level=level)
            logger.debug(f'level_before:{level_rules}')
            level_rules = self.get_level_rules_v1(level_rules,self.process_result_classified_file)
            parent_rule_name_list = list(set(level_rules.get_parent_rules_name_by_level(level)))
            logger.debug(f'level:{level}')
            logger.debug(f'level_rules_after:{level_rules}')
            logger.debug(f'parent_rule_name_list:{parent_rule_name_list}')
            level_results = {}
            
            # 处理每个阶段1文件
            for output_name, values in self.process_result_classified_file.items():
                file_path = values['file_path']
                if values.get('classified_sheet_name') is None:
                    continue
                classified_sheet_name_list = values['classified_sheet_name']
                # 读取前一阶段分类文件
                pr_level_dict = self.excel_handler.read_stage_results(file_path)

                for classified_sheet_name in classified_sheet_name_list:
                    for parent_rule_name in parent_rule_name_list:
                        #获取需要分类的关键词
                        
                        unclassified_keyword = self._process_stage_df(pr_level_dict,level,classified_sheet_name = classified_sheet_name,parent_rule=parent_rule_name)
                        logger.debug(f'classified_sheet_name:{classified_sheet_name},parent_rule_name:{parent_rule_name},level:{level}，unclassified_keyword:{unclassified_keyword}')
                        if unclassified_keyword is None:
                            continue

                        # 获取分类规则

                        output_name_rules = level_rules.filter_rules(output_name=output_name,classified_sheet_name=classified_sheet_name,parent_rule=parent_rule_name)

                        if output_name_rules:
                            classified_result = self._get_classified_results(unclassified_keyword,output_name_rules,level)
                            # if stage3_results == {}:
                            #     stage3_results[output_name] = {}
                            # elif stage3_results.get(output_name) is None:
                            #     stage3_results[output_name] = {}
                            # else:
                            #     stage3_results[output_name][classified_sheet_name] = classified_result
                            level_results.setdefault(output_name, {}).setdefault(classified_sheet_name, {})[parent_rule_name] = classified_result
                        else:
                            msg = f'找不到{output_name}的Sheet2规则，已经返回'
                            if self.error_callback:
                                self.error_callback(msg)
            return level_results
        except Exception as e:
            raise Exception(f"处理阶段3失败: {str(e)}")


    def save_stage_high_results(self, level:int,stage_high_result:Dict) -> dict[str, Path]:
        """保存阶段3分类结果
        
        Args:
            classified_result: 分类结果
            error_callback: 错误回调函数
            
        Returns:
            保存的文件路径字典
        """
        try:
            classified_result:Optional[models.ClassifiedResult] = None
            for output_name,result_dict in self.process_result_classified_file.items():
                if result_dict == {}:
                    continue
                file_path = result_dict['file_path']

                
                for classified_sheet_name,parent_result in stage_high_result.get(output_name,{}).items():
                    for parent_rule_name,classified_result in parent_result.items():
                        logger.debug(f'\n\nclassified_sheet_name: {classified_sheet_name}\n\n')
                        logger.debug(f'\n\nparent_rule_name: {parent_rule_name}\n\n')
                        logger.debug(f'\n\nclassified_result: {classified_result}\n\n')
                        if classified_result is None:
                            continue
                        # 构建 keyword 到 matched_rule 的映射
                        filtered_result = classified_result.filter(classified_conditions={'classified_sheet_name':classified_sheet_name,'parent_rule':parent_rule_name})
                        if filtered_result is None:
                            continue
                        keyword_to_rule = {
                            kw.keyword: kw.matched_rule 
                            for kw in filtered_result.classified_keywords
                        }
                        logger.debug(f'\n\nkeyword_to_rule: {keyword_to_rule}\n\n')
                        self.add_matched_rule_with_pandas(excel_path = file_path,
                                                        sheet_name = classified_sheet_name,
                                                        keyword_to_rule = keyword_to_rule,
                                                        new_column_name = '阶段'+str(level)
                                                        )
            return True
        except  Exception as e:
            err_msg = f'保存阶段三分类结果失败：{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(err_msg)

    def process_workflow(self, rules_file: Path, classification_file: Path, error_callback=None):
        """处理完整工作流
        
        Args:
            rules_file: 工作流规则文件路径
            classification_file: 待分类文件路径
            error_callback: 错误回调函数
            
        Returns:
            生成的文件路径字典
        """
        try:
            result = {}
            stage = 1
            # 读取工作流规则
            workflow_rules = self.excel_handler.read_workflow_rules(rules_file)
            self.workflow_rules = workflow_rules
            logger.debug(f'self.workflow_rules: {self.workflow_rules}')    
            # 读取待分类文件
            unclassified_keywords = self.excel_handler.read_keyword_file(classification_file)
            # 处理阶段1：基础分类,将词分类到各xlsx文件中
            stage1_results = self.process_stage1(unclassified_keywords, workflow_rules, error_callback)
            
            # 保存阶段1结果
            stage1_files = self.save_stage1_results(stage1_results)
            self.process_result_file = stage1_files
            result = {'stage':1,'result':stage1_files}
            stage += 1
            max_level = workflow_rules.get_max_level()
            logger.debug(f'max_level: {max_level}')
            if stage <= max_level:
                # 处理阶段2：将分类细分到各sheet
                stage2_results = self.process_stage2(stage1_files, workflow_rules, error_callback)
                # 保存阶段2结果
                stage2_files = self.save_stage2_results(stage1_files, stage2_results, error_callback)
                result = {'stage':2,'result':stage2_files}
                stage += 1
                logger.debug(f'当前工作流层级: {stage},max_level: {max_level}')
            if stage <= max_level:
                self.process_result_classified_file = self.excel_handler.read_stage_classified_sheet_name(self.process_result_file)
                logger.debug(f'self.process_result_classified_file:{self.process_result_classified_file}')
                # 处理阶段3：分类后处理（Sheet3处理）
                stage3_results = self.process_stage3(stage2_files, workflow_rules, error_callback)
                
                stage3_file = self.save_stage3_results(stage2_file=stage2_files,stage3_results=stage3_results,error_callback=error_callback)
                result = {'stage':3,'result':stage3_file}
                stage += 1
                logger.debug(f'当前工作流层级: {stage},max_level: {max_level}')
            while stage <= max_level:
                stage_result = self.process_stage_high(stage)
                stage_save_result = self.save_stage_high_results(stage,stage_result)
                result = {'stage':stage,'result':stage_save_result}
                stage += 1
                logger.debug(f'stage_result:{stage_result}')
            logger.debug(f'result:{result}')
            return result
            

        except Exception as e:
            err_msg = f'处理完整工作流失败：{e}'
            if error_callback:
                error_callback(err_msg)
            raise Exception(f"处理完整工作流失败：{e}")
