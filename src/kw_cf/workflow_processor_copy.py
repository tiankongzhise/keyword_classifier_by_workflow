from pathlib import Path

from .keyword_classifier import KeywordClassifier
from .excel_handler import ExcelHandler
from typing import Optional,Callable,cast,Any,Literal
from . import models
from . import utils
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
        
        self.tools:utils.WorkFlowProcessorUtil = utils.WorkFlowProcessorUtil(self)
        
        self.max_process_level:int = 0 # 工作流运行长度 
        
        self.level:int = 0 # 当前工作流运行长度

        self.output_dir = Path('./工作流结果')# 输出文件夹
        
        self.output_dir.mkdir(parents=True, exist_ok=True)# 创建文件夹
    
    

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
            classified_reuslt =  self.tools.trans_words_to_cassified_result(classify_result,workflow_rules)
            
            return classified_reuslt
        except Exception as e:
            msg = f"_get_classified_results出错: {e}"
            if self.error_callback:
                self.error_callback(msg)
            raise Exception(msg) from e
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
                        df = self.tools.transform_to_df(unclassify_keyword_list)
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
                        df = self.tools.transform_to_df(matched_keyword_list)
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
