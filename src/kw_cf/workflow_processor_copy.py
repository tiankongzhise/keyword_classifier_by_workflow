from pathlib import Path

from .keyword_classifier import KeywordClassifier
from .excel_handler import ExcelHandler
from typing import Optional,Callable,cast,Any
from . import models
from . import utils                          
                          
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
        
    
        self.error_callback:Optional[Callable] = error_callback # 错误回调函数
        
        self.workflow_rules:models.WorkFlowRules = None # 全部工作流规则 #type:ignore
        
        self.process_file_path:models.ProcessFilePaths = None #包含分类sheet的运行结果 #type:ignore
        
        self.tools:utils.WorkFlowProcessorUtil = utils.WorkFlowProcessorUtil(self)
        
        self.max_process_level:int = 0 # 工作流运行长度 
        
        self.level:int = 0 # 当前工作流运行长度

        self.output_dir = Path('./工作流结果')# 输出文件夹
        
        self.output_dir.mkdir(parents=True, exist_ok=True)# 创建文件夹
    
    

    def classfy_keyword(self,
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

    def add_level(self)->None:
        self.level += 1
    
    def is_next_process(self)->bool:
        '''
        判断是否需要继续处理
        '''
        return self.level <= self.max_process_level
    
    def set_level_end(self)->None:
        self.level = 9999
    
    
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
            self.workflow_rules = workflow_rules
            # 获取最大工作流级数
            max_process_level = workflow_rules.max_process_level()
            # 存储最大工作流级数
            self.max_process_level = max_process_level
            # 读取待分类的关键词
            unclassified_keywords = self.excel_handler.read_keyword_file(classification_file)
            return unclassified_keywords
        except Exception as e:
            err_msg = f"Error in pre_work: {e}"
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(err_msg )from e

    
    def process_stage1(self,keywords:models.UnclassifiedKeywords)->models.ProcessReturnResult:
        """处理第一阶段的关键词分类"""
        # 获取一阶段分类规则
        stage1_rules = cast(models.WorkFlowRules,self.workflow_rules.get(1))
        # 分类关键词
        classification_result = self.classfy_keyword(keywords,stage1_rules)
        if classification_result.is_empty(classified_type='classified_keywords'):
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,'第一阶段分类结果为空，请检查关键词分类规则是否正确')
        
        # 获取分类结果
        unmatched_keywords = self.tools.get_classification_groups(classification_result,mode='output_name',keyword_status='unmatch')
        matched_keywords = self.tools.get_classification_groups(classification_result,mode='output_name',keyword_status='match')
        
        # 保存分类失败的关键词
        if unmatched_keywords:
            for output_name, unclassify_keyword_list in unmatched_keywords.items():
                output_file = self.tools.generate_timestamped_path(self.output_dir,cast(str,output_name))
                df = self.tools.transform_to_df(unclassify_keyword_list)
                self.excel_handler.save_results(df, output_file,sheet_name='Sheet1')

        temp_process_return_result_dict = self.tools.create_temp_process_return_result_dict(self.level)
        temp_process_path_file_list = []
        # 保存分类成功的关键词
        if matched_keywords:
            classified_sheet_name = 'Sheet1'
            for output_name, matched_keyword_list in matched_keywords.items():
                output_file:Path = self.tools.generate_timestamped_path(self.output_dir,cast(str,output_name))
                df = self.tools.transform_to_df(matched_keyword_list)
                self.excel_handler.save_results(df, output_file,sheet_name=classified_sheet_name)
                level = self.level
                temp_process_path_file_list.append(models.ProcessFilePath(file_path=output_file,level=level,output_name=cast(str,output_name),classified_sheet_name=classified_sheet_name))
                temp_process_return_result_dict['process_sheet_count'] += 1
                temp_process_return_result_dict['sheet_status_counts']['success'] += 1
                
        # 防止输出空文件
        if not temp_process_path_file_list:
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,'第一阶段分类结果为有值,但输出的结果为空，请检查process_stage1关于matched_keywords的代码')
            
        # 将成功分类的文件更新存储在self对象中,供后续阶段使用
        self.process_file_path = models.ProcessFilePaths(file_paths=temp_process_path_file_list)
        temp_process_return_result_dict['status'] = 'success'
        temp_process_return_result_dict['info'] = '第一阶段分类成功'
        return models.ProcessReturnResult(**temp_process_return_result_dict)



            
    

    def process_stage2(self)->models.ProcessReturnResult:
        # 预处理,如果一阶段没有成功分类的文件，则直接返回失败
        if self.process_file_path is None:
            msg = 'stage1没有分类成功的关键词,请检查关键词分类规则是否正确'
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,msg)
        
        # 获取分类规则
        workflow_rules = self.workflow_rules
        level_workflow_rules = workflow_rules.get(self.level)
        if level_workflow_rules is None:
            msg = f'stage2获取分类规则失败,level：{self.level}'
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,msg)
                
        # 生成临时结果dict,用来保存分类结果
        temp_process_return_result_dict = self.tools.create_temp_process_return_result_dict(self.level)

        # 生成临时结果文件路径
        temp_process_path_file_list = []
        
        
        # 进行阶段二分类
        for process_item in self.process_file_path.file_paths:
            file_path = process_item.file_path
            source_file_name = process_item.output_name
            source_sheet_name = process_item.classified_sheet_name or 'Sheet1'
            level = self.level
            
            # 获取未分类关键词
            result_df = self.excel_handler.read_stage_result(file_path,source_sheet_name)
            unclassifie_keywords = self.tools.get_unclassified_keywords_from_result_df(result_df,source_file_name,source_sheet_name,level)
            
            # 检查类型问题，如果为空，则跳过该文件
            if unclassifie_keywords is None:
                msg = f'stage2文件：{file_path},sheet：{source_sheet_name},获取未分类关键词返回了空表'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,status='warning',output_file_name=file_path.name,output_sheet_name=source_sheet_name,info=msg)
                continue
            
            output_name_workflow_rules = level_workflow_rules.filter_rules(output_name=source_file_name)
            if output_name_workflow_rules is None:
                msg = f'stage2:output_name：{source_file_name}没有需要分类的规则'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,'warning',msg,source_file_name,source_sheet_name)
                continue


            # 获取分类结果
            classified_result = self.classfy_keyword(unclassifie_keywords,output_name_workflow_rules)
            
            if classified_result.is_empty('classified_keywords'):
                msg = f'stage2,文件：{source_file_name},sheet：{source_sheet_name},没有分类成功的关键词'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,'warning',msg,source_file_name,source_sheet_name)
                continue
            
            unmatched_keywords = self.tools.get_classification_groups(classified_result,'sheet','unmatch')
            matched_keywords = self.tools.get_classification_groups(classified_result,'sheet','match')
            
            # 保存分类失败的关键词
            if unmatched_keywords:
                for tuple_item, unclassify_keyword_list in unmatched_keywords.items():
                    output_name, output_sheet_name = tuple_item
                    output_file = file_path
                    df = self.tools.transform_to_df(unclassify_keyword_list)
                    self.excel_handler.add_sheet_data(df, output_file,sheet_name=output_sheet_name)

            temp_process_return_result_dict = self.tools.create_temp_process_return_result_dict(self.level)
            
            # 保存分类成功的关键词
            if matched_keywords:
                for tuple_item, matched_keyword_list in matched_keywords.items():
                    output_name, classified_sheet_name = tuple_item
                    output_file:Path = file_path
                    df = self.tools.transform_to_df(matched_keyword_list)
                    self.excel_handler.add_sheet_data(df, output_file,sheet_name=classified_sheet_name)
                    level = self.level
                    temp_process_path_file_list.append(models.ProcessFilePath(file_path=output_file,level=level,output_name=cast(str,output_name),classified_sheet_name=classified_sheet_name))
                    self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,'success',f'stage2,文件：{source_file_name},sheet：{source_sheet_name},分类成功',output_name,classified_sheet_name)
                    
        # 防止输出空文件
        if not temp_process_path_file_list:
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,'第二阶段分类结果为有值,但输出的结果为空，请检查process_stage2关于matched_keywords的代码')
                
        # 将成功分类的文件更新存储在self对象中,供后续阶段使用
        self.process_file_path = models.ProcessFilePaths(file_paths=temp_process_path_file_list)
        temp_process_return_result_dict['info'] = '第二阶段分类成功'
        return models.ProcessReturnResult(**temp_process_return_result_dict)
            
   
    def process_stage3(self)->models.ProcessReturnResult:
        # 预处理,如果一阶段没有成功分类的文件，则直接返回失败
        if self.process_file_path is None:
            msg ='stage1没有分类成功的关键词,请检查关键词分类规则是否正确'
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,msg)
        
        # 获取分类规则
        workflow_rules = self.workflow_rules
        level_workflow_rules = workflow_rules.get(self.level)
        if level_workflow_rules is None:
            msg = f'stage3获取分类规则失败,level：{self.level}'
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,msg)

        # 生成临时结果dict,用来保存分类结果
        temp_process_return_result_dict = self.tools.create_temp_process_return_result_dict(self.level)
        # 生成临时结果文件路径
        temp_process_path_file_list = []
        
        # 进行阶段三分类
        for process_item in self.process_file_path.file_paths:
            file_path = process_item.file_path
            source_file_name = process_item.output_name
            source_sheet_name = process_item.classified_sheet_name or 'Sheet1'
            level = self.level
            
            # 获取待分类关键词
            result_df = self.excel_handler.read_stage_result(file_path,source_sheet_name)
            unclassifie_keywords = self.tools.get_unclassified_keywords_from_result_df(result_df,source_file_name,source_sheet_name,level)

            # 检查类型问题，如果为空，则跳过该文件
            if unclassifie_keywords is None:
                msg = f'stage3文件：{file_path},sheet：{source_sheet_name},获取未分类关键词返回了空表'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,status='warning',output_file_name=file_path.name,output_sheet_name=source_sheet_name,info=msg)
                continue
            
            # 获取分类规则
            sheet_workflow_rules = level_workflow_rules.filter_rules(output_name=lambda x:x==source_file_name or x=='全',classified_sheet_name=lambda x:x==source_sheet_name or x=='全',rule_tag = lambda x:x is not None)
            if sheet_workflow_rules is None:
                msg = f'stage3,source_file:{source_file_name},sheet：{source_sheet_name},没有需要分类的规则'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,status='warning',output_file_name=source_file_name,output_sheet_name=source_sheet_name,info=msg)
                continue
            

            # 获取分类结果
            classified_result = self.classfy_keyword(unclassifie_keywords,sheet_workflow_rules)
        
            if classified_result.is_empty('classified_keywords'):
                msg = f'stage3,文件：{source_file_name},sheet：{source_sheet_name},没有匹配任何一条规则'
                self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,'warning',msg,source_file_name,source_sheet_name)
                continue

            matched_keywords = self.tools.get_classification_groups(classified_result,'sheet','match')
            temp_process_return_result_dict = self.tools.create_temp_process_return_result_dict(self.level)
            
            # 保存分类成功的关键词
            if matched_keywords:
                for tuple_item, matched_keyword_list in matched_keywords.items():
                    output_name, classified_sheet_name = tuple_item
                    source_file_path = file_path
                    output_file:Path = self.tools.generate_timestamped_path(base_dir=self.output_dir,filename=output_name)
                    tag_column_name = matched_keyword_list[0].rule_tag_column
                    df = self.tools.transform_to_df(matched_keyword_list)
                    self.excel_handler.add_sheet_columns(excel_path=source_file_path,sheet_name=classified_sheet_name,new_df=df,key_mapping={'关键词':'关键词'},tag_mapping={tag_column_name:tag_column_name})
                    level = self.level
                    temp_process_path_file_list.append(models.ProcessFilePath(file_path=output_file,level=level,output_name=cast(str,output_name),classified_sheet_name=classified_sheet_name))
                    self.tools.update_temp_process_return_result_dict(temp_process_return_result_dict,'success',f'stage3,文件：{source_file_name},sheet：{source_sheet_name},分类成功',output_name,classified_sheet_name)
        # 防止输出空文件
        if not temp_process_path_file_list:
            self.set_level_end()
            return self.tools.create_fail_process_return_result(self.level,'第三阶段分类结果为有值,但输出的结果为空，请检查process_stage3关于matched_keywords的代码')
                
        # 将成功分类的文件更新存储在self对象中,供后续阶段使用
        self.process_file_path = models.ProcessFilePaths(file_paths=temp_process_path_file_list)
        temp_process_return_result_dict['info'] = '第三阶段分类成功'
        return models.ProcessReturnResult(**temp_process_return_result_dict)

            
    def process_workflow(self, rules_file: Path, classification_file: Path)->models.ProcessReturnResult:
        """处理完整工作流
        
        Args:
            rules_file: 工作流规则文件路径
            classification_file: 待分类文件路径
            error_callback: 错误回调函数
            
        Returns:
            models.ProcessLevelResult,处理结果
        """
        try:
            stage_result = self.tools.create_fail_process_return_result(level=self.level,info="初始化WorkFlowProcessor,未进行任何处理")
            # 预处理工作，设置self.workflow_rules和获取最初的待分类关键词
            unclassified_keywords = self.pre_work(rules_file=rules_file,classification_file=classification_file)
            if unclassified_keywords.is_empty():
                err_msg = "执行完预处理工作后，发现待分类关键词为空"
                self.set_level_end()
                return self.tools.create_fail_process_return_result(self.level,err_msg)
            
            # 执行完预处理 进入下一阶段,由预处理进入阶段一
            self.add_level()

            # 判断是否有下一阶段规则,执行相应处理
            if self.is_next_process():
                # 处理阶段1：基础分类,将词分类到各xlsx文件中
                stage_result = self.process_stage1(unclassified_keywords)
            
            # 执行完阶段一 进入下一阶段
            self.add_level()

            # 判断是否有下一阶段规则,执行相应处理
            if self.is_next_process():
                # 处理阶段2：分类结果处理,将分类结果保存到excel中
                stage_result = self.process_stage2()
            print(stage_result)
            # 执行完阶段二 进入下一阶段
            self.add_level()
            if self.is_next_process():
                # 处理阶段3：分类结果处理,将分类结果保存到excel中
                stage_result = self.process_stage3()  
            
            
            
            return stage_result
            
            

        except Exception as e:
            err_msg = f'处理完整工作流失败：{e}'
            if self.error_callback:
                self.error_callback(err_msg)
            raise Exception(f"处理完整工作流失败：{e}")
