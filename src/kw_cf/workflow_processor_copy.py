from pathlib import Path

from .keyword_classifier import KeywordClassifier
from .excel_handler import ExcelHandler
from .logger_config import logger
from typing import List,Dict,Optional,Callable,cast,Any,Literal,Mapping
from . import models
import pandas as pd
import datetime


class GetProcessLevelResult(object):
    def __init__(self,parent_self:'WorkFlowProcessor'):
        self.parent_self = parent_self
    
    
    def success(self,
                status:Literal["success", "warning"],
                message:str|None = None,
                next_level:Optional[int] = None,
                )->models.ProcessLevelResult:
        '''
        封装返回处理成功的结果,会自动从实例中读取level，如果未设置下一级则自动设置为当前level+1，更新实例的level
        args:
            status: 处理状态
            message: 处理结果信息
            next_level: 下一级工作流运行长度 9999标识结束，如果未输入，则为当前level+1
        reuturn:
            ProcessResult: 处理结果
        '''
        level = self.parent_self.get('level')
        if next_level is None:
            self.parent_self.add_level()
            next_level = self.parent_self.get('level')
        else:
            self.parent_self.set('level',next_level)
        return models.ProcessLevelResult(
                level=level,
                status=status,
                message=message,
                next_level=cast(int,next_level)
            )
    def fail(self,
             message:str,
             )->models.ProcessLevelResult:
        '''
        封装返回处理失败的结果,会自动从实例中读取level，并自动设置下一级level为9999，并且会更新实例的level
        args:
            status: 处理状态
            message: 处理结果信息
        reuturn:
            ProcessResult: 处理结果
        '''
        level = self.parent_self.get('level')
        self.parent_self.set('level',9999)
        return models.ProcessLevelResult(
            level=level,
            status='fail',
            message=message,
            next_level=9999  
        )
             
class GetProcessTempResult(object):
    def __init__(self,parent_self:'WorkFlowProcessor'):
        self.parent_self = parent_self
    
    
    def success(self,
                status:Literal["success", "warning"],
                data:models.ClassifiedResult,
                message:str|None = None,
                )->models.ProcessTempResult:
        '''
        封装返回处理成功的结果,会自动从实例中读取level，如果未设置下一级则自动设置为当前level+1，不会更新实例的level
        args:
            status: 处理状态
            message: 处理结果信息
            next_level: 下一级工作流运行长度 9999标识结束，如果未输入，则为当前level+1
        reuturn:
            ProcessResult: 处理结果
        '''
        level = self.parent_self.get('level')
        return models.ProcessTempResult(
                level=level,
                status=status,
                data=data,
                message=message,
            )
    def fail(self,
             message:str,
             data:models.ClassifiedResult|None = None,
             )->models.ProcessTempResult:
        '''
        封装返回处理失败的结果,会自动从实例中读取level，并自动设置下一级level为9999，并且会更新实例的level
        args:
            status: 处理状态
            message: 处理结果信息
        reuturn:
            ProcessResult: 处理结果
        '''
        level = self.parent_self.get('level')
        return models.ProcessTempResult(
            level=level,
            status='fail',
            message=message,
            data=data
        )
                          
    

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
        self.excel_handler:ExcelHandler = excel_handler or ExcelHandler(error_callback) # Excel处理器实例
        self.classifier:KeywordClassifier = keyword_classifier or KeywordClassifier(error_callback=error_callback) # 创建关键词分类器实例
        self.get_process_result = GetProcessLevelResult(self) # 封装返回处理结果
        self.get_process_temp_result = GetProcessTempResult(self) # 封装返回过程结果
        self.error_callback:Optional[Callable] = error_callback # 错误回调函数
        self.workflow_rules:models.WorkFlowRules = None # 全部工作流规则 #type:ignore
        self.process_file_path:models.ProcessFilePaths = None #包含分类sheet的运行结果 #type:ignore
        self.max_process_level:int = 0 # 工作流运行长度 
        self.level:int = 0 # 当前工作流运行长度

        self.output_dir = Path('./工作流结果')# 输出文件夹
        self.output_dir.mkdir(parents=True, exist_ok=True)# 创建文件夹
    
    
    def _transfrom_unmathced_keywords(self,unmatched_keywords:List[models.UnMatchedKeyword])->pd.DataFrame:
        result = []
        for unmatched_keyword in unmatched_keywords:
            temp_dict = {}
            temp_dict['关键词'] = unmatched_keyword.keyword
            temp_dict['分类层级'] = unmatched_keyword.level
            temp_dict['来源sheet'] = unmatched_keyword.source_sheet_name
            result.append(temp_dict)
        return pd.DataFrame(result)
    
    def _transfrom_classified_keywords(self,classified_keywords:List[models.ClassifiedKeyword])->pd.DataFrame:
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
                
            
        

    def _transform_to_df(self,data:List[models.UnMatchedKeyword|models.ClassifiedKeyword])->pd.DataFrame:
        map_func = {
            models.UnMatchedKeyword:self._transfrom_unmathced_keywords,
            models.ClassifiedKeyword:self._transfrom_classified_keywords
        }
        return map_func[type(data[0])](data)
        
        
    def _trans_words_to_cassified_result(self,classify_result:List[models.ClassifiedWord],workflow_rules:models.WorkFlowRules)->models.ClassifiedResult:
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
        
    def _get_classified_results(self,
                                unclassified_keywords:models.UnclassifiedKeywords,
                                workflow_rules:models.WorkFlowRules)->models.ClassifiedResult:
        """关键词分类
        
        Args:
            keywords: 未分类关键词
            workflow_rules: 工作流规则
            error_callback: 错误回调函数
        
        Returns:
            ClassifiedResult:分类结果
        """
        try:
            
            # 获取分类规则列表，方便后续处理
            rules = workflow_rules.to_rules_list()
            
            # 设置分类规则
            self.classifier.set_rules(models.SourceRules(data=rules,error_callback=self.error_callback))
            
            # 分类关键词
            classify_result = self.classifier.classify_keywords(unclassified_keywords)
            
            # 转换分类结果
            classified_reuslt =  self._trans_words_to_cassified_result(classify_result,workflow_rules)
            
            return classified_reuslt
        except Exception as e:
            msg = f"_get_classified_results出错: {e}"
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg) from e
        
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
                                      error_callback=None)->Optional[models.WorkFlowRules]:
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
    def _special_rules_match_process_v1(self,workflow_rules:models.WorkFlowRules,stage_results:Dict)->Optional[models.WorkFlowRules]:
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
        keyword_to_rule:Mapping[str,str],
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
            df[new_column_name] = df["关键词"].map(keyword_to_rule) #type:ignore
            
            # 保存回原文件
            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)   
            return True
        except Exception as e:
            err_msg = f'add_matched_rule_with_pandas 保存文件失败{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(err_msg)
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
    
    def get(self, attr_name:str)->Any:
        '''
        获取实例对象属性，如果属性不存在，则返回None
        args:
            attr: 属性名
        return:
            属性值 or None
        '''
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
    
    def add_level(self)->None:
        self.set('level',self.get('level')+1)
    
    def is_next_process(self)->bool:
        '''
        判断是否需要继续处理
        '''
        return self.get('level') <= self.get('max_process_level')
    
    def set(self, attr_name:str, data: Any)->Any:
        '''
        设置类对象的属性，如果属性不存在，则返回None,如果属性存在，则返回属性值
        args:
            attr: 属性名
            data: 属性值
        return:
            属性值 or None
        '''
        if hasattr(self, attr_name):
            setattr(self, attr_name, data)
            return getattr(self, attr_name)
        else:
            err_msg = f"{attr_name} is not a attribute of {self.__class__.__name__}"
            if self.error_callback:
                self.error_callback(err_msg)
            return None

    def process_stage1(self,keywords:models.UnclassifiedKeywords)->models.ProcessTempResult:
        """处理第一阶段的关键词分类"""
        try:
            # 获取一阶段分类规则
            stage1_rules = cast(models.WorkFlowRules,self.workflow_rules.get(1))
            # 分类关键词
            result = self._get_classified_results(keywords,stage1_rules)
            if result.is_empty(classified_type='classified_keywords'):
                msg = '第一阶段关键词分类结果为空'
                if self.error_callback:
                    self.error_callback(msg)
                raise Exception(msg)
            return self.get_process_temp_result.success(status='success',data=result)
        except Exception as e:
            msg = f"获取一阶段分类规则失败：{e}"
            if self.error_callback:
                self.error_callback(f"获取一阶段分类规则失败：{e}")
            raise Exception(msg) from e

    def save_stage1_results(self,classified_result:models.ProcessTempResult)->models.ProcessLevelResult:
        """保存第一阶段分类结果
        
        Args:
            classified_result:models.ProcessTempResult, 分类结果
                args:
                    level:int 阶段
                    status:str 处理结果
                    data:ClassifiedResult 处理结果
                    message:str|None 错误信息
        Returns:
            models.ProcessLevelResult
                args:
                    level:int 阶段
                    status:Literal["success", "fail", "warning"] 处理结果
                    next_level:int 下一个阶段名称,9999标识结束
                    message:str|None 错误信息
        """
        temp_process_path_file_list = []
        try:
            if classified_result.data is None or classified_result.data.is_empty(classified_type='classified_keywords'):
                msg = '第一阶段分类结果为空，请检查关键词分类规则是否正确'
                if self.error_callback:
                    self.error_callback(msg)
                raise Exception(msg)

            # 获取分类结果
            unmatched_keywords = classified_result.data.get_grouped_keywords(group_by='output_name',match_type='unmatch')
            matched_keywords = classified_result.data.get_grouped_keywords(group_by='output_name',match_type='match')
            
            try:
                # 保存分类失败的关键词
                if unmatched_keywords:
                    for output_name, unclassify_keyword_list in unmatched_keywords.items():
                        output_file = self.output_dir / f'{output_name}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                        df = self._transform_to_df(unclassify_keyword_list)
                        self.excel_handler.save_results(df, output_file,sheet_name='Sheet1')
            except Exception as e:
                err_msg = f'保存分类失败的关键词失败：{e}'
                if self.error_callback:
                    self.error_callback(err_msg)
                raise Exception(f"保存分类失败的关键词失败：{e}")
            try:
                if matched_keywords:
                    classified_sheet_name = 'Sheet1'
                    for output_name, matched_keyword_list in matched_keywords.items():
                        output_file:Path = self.output_dir / f'{output_name}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                        df = self._transform_to_df(matched_keyword_list)
                        self.excel_handler.save_results(df, output_file,sheet_name=classified_sheet_name)
                        level = self.get('level') + 1 
                        temp_process_path_file_list.append(models.ProcessFilePath(file_path=output_file,level=level,output_name=cast(str,output_name),classified_sheet_name=classified_sheet_name))
                    self.process_file_path = models.ProcessFilePaths(file_paths=temp_process_path_file_list)
                msg = '第一阶段分类结果保存成功'
                if self.error_callback:
                    self.error_callback(msg)
                return self.get_process_result.success(status='success',message=msg)
            except Exception as e:
                err_msg = f'保存分类成功的关键词失败：{e}'
                if self.error_callback:
                    self.error_callback(err_msg)
                raise Exception(f"保存分类成功的关键词失败：{e}")
        except Exception as e:
            err_msg = f'获取分类结果失败：{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(f"获取分类结果失败：{e}")

    
    def pre_work(self, rules_file: Path, classification_file: Path)->models.UnclassifiedKeywords:
        '''
        执行peocess预处理工作,获取规则和分类文件，获取最大工作流级数
        由于规则与最大工作流级数后续多次使用，存储在self对象中，未分类关键词只使用本次，因此直接返回
        args:
            rules_file: Path,规则文件路径
            classification_file: Path,分类文件路径
        return:
            unclassified_keywords: UnclassifiedKeywords,未分类关键词
        '''
        
        try:
            # 读取工作流规则
            workflow_rules = self.excel_handler.read_workflow_rules(rules_file)
            # 存储规则
            self.set('workflow_rules', workflow_rules)
            # 获取最大工作流级数
            max_process_level = workflow_rules.max_process_level()
            # 存储最大工作流级数
            self.set('max_process_level', max_process_level)
            # 读取待分类的关键词
            unclassified_keywords = self.excel_handler.read_keyword_file(classification_file)
            return unclassified_keywords
        except Exception as e:
            err_msg = f"Error in pre_work: {e}"
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(err_msg )from e

    def process_workflow(self, rules_file: Path, classification_file: Path)->models.ProcessLevelResult:
        """处理完整工作流
        
        Args:
            rules_file: 工作流规则文件路径
            classification_file: 待分类文件路径
            error_callback: 错误回调函数
            
        Returns:
            models.ProcessLevelResult,处理结果
        """
        stage_result = self.get_process_result.success(status='success',message='process 初始化',next_level=0)
        try:
            
            # 预处理工作，设置self.workflow_rules和获取最初的待分类关键词
            unclassified_keywords = self.pre_work(rules_file=rules_file,classification_file=classification_file)
            if unclassified_keywords.is_empty():
                err_msg = "执行完预处理工作后，发现待分类关键词为空"
                if self.error_callback:
                    self.error_callback(err_msg)
                return self.get_process_result.success(status='warning',message=err_msg,next_level=9999)
            
            # 执行完预处理 进入下一阶段,由预处理进入阶段一
            self.add_level()

            if self.is_next_process():
                # 处理阶段1：基础分类,将词分类到各xlsx文件中
                stage1_temp_result = self.process_stage1(unclassified_keywords)
                stage_result = self.save_stage1_results(stage1_temp_result)

            return stage_result
            
            

        except Exception as e:
            err_msg = f'处理完整工作流失败：{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(f"处理完整工作流失败：{e}")
