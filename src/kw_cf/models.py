from pydantic import BaseModel, field_validator, Field,ValidationInfo,model_validator
from .logger_config import logger
from pathlib import Path

from typing import List, Optional, Callable, Any,Literal,Dict


__all__ = [
    "UnclassifiedKeywords",
    "SourceRules",
    'ClassifiedWord',
    'WorkFlowRule',
    'WorkFlowRules',
    'ClassifiedKeyword',
    'UnMatchedKeyword',
    'ClassifiedResult'
]


def _preprocess_text(text, error_callback=None):
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

            logger.debug(msg)

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

        logger.debug(msg1)

        logger.debug(msg2)

        if error_callback:
            error_callback(msg1)

            error_callback(msg2)

    return cleaned_rule if has_invisible else text


def _preserve_order_deduplicate(lst: List[str]) -> List[str]:
    """保序去重函数（兼容Python 3.6+）"""

    seen = set()

    return [x for x in lst if not (x in seen or seen.add(x))]


class UnclassifiedKeywords(BaseModel):
    '''
    未分类关键词,对传入的未分类关键词进行预处理,去除不可见字符串，空值过滤,并保序去重
    对来源进行验证，当level为1时，不验证来源文件和sheet,因为level为1时，输入为用户手动选择的待分类文件，验证无意义。
    当level>=1,需要有来源文件，标明来自哪个分类文件
    当level>=2,需要有来源sheet,标明来自哪个分类sheet

    args:
        data:List[str] 未分类关键词
        souce_file_name:str|None 关键词来源文件名称
        souce_sheet_name:str|None 关键词来源sheet名称
        level:工作流层级，从0开始，0表示最初的待分类关键词
        error_callback:错误信息回调函数
    return:
        UnclassifiedKeywords:未分类关键词
    '''
    data: List[str] # 未分类关键词
    source_file_name: str|None = Field(None, min_length=1, description="关键词来源文件名称" ) # 关键词来源文件名称
    source_sheet_name: str | None = Field(None, min_length=1, description="关键词来源sheet名称" )# 关键词来源sheet名称
    level: int = Field(..., ge=0, description="未分类关键词层级" ) # 未分类关键词层级
    error_callback: Optional[Callable[..., Any]] = Field(
        None, exclude=True, description="错误信息回调函数"
    )


    @field_validator("data", mode='before')
    def processing_pipeline(cls, v: Any, info: ValidationInfo) -> List[str]:
        """处理流水线：类型转换 -> 预处理 -> 空值过滤 -> 保序去重"""

        # 类型安全转换

        if not isinstance(v, (list, tuple, set)):
            raise ValueError("输入必须是可迭代对象")

        # 保留原始数据

        keyword_list = [str(item) for item in v]

        # 预处理流水线
        error_callback = info.data.get("error_callback")

        processed = [
            _preprocess_text(keyword, error_callback).strip()  # 移除首尾空格
            for keyword in keyword_list
        ]

        # 空值过滤（包括空白字符）

        non_empty = [keyword for keyword in processed if keyword]

        # 保序去重逻辑

        return _preserve_order_deduplicate(non_empty)

    def is_empty(self) -> bool:
        """判断是否为空"""

        return not bool(self.data)
    
    @model_validator(mode = 'after')
    def validate_rules(self)->'UnclassifiedKeywords':
        """验证规则"""
        err_msg = []
        if self.level > 1 and not self.source_file_name:
            err_msg.append(f"未分类关键词 {self.data} 的层级大于1，但没有指定来源文件名称")
        if self.level > 2 and not self.source_sheet_name:
            err_msg.append(f"未分类关键词 {self.data} 的层级大于等于2，但没有指定来源sheet名称")
        if err_msg:
            if self.error_callback:
                self.error_callback("\n".join(err_msg))
            raise ValueError("\n".join(err_msg))
        return self
    
    class Config:
        validate_assignment = True  # 允许在赋值时触发验证


class ClassifiedWord(BaseModel):
    '''关键词分类中间状态
    args:
        keyword:str 关键词
        matched_rule:str 匹配的规则,未匹配任何规则是为''
        source_file_name:str|None 关键词来源文件名称
        source_sheet_name:str|None 关键词来源sheet名称
        level:int 工作流层级，从0开始，0表示最初的待分类关键词
    return:
        ClassifiedWord:关键词分类中间状态
    '''
    keyword: str
    matched_rule:str
    source_file_name:str|None = Field(None,min_length=1,description="关键词来源文件名称")# 关键词来源文件名称
    source_sheet_name:str|None = Field(None,min_length=1,description="关键词来源sheet名称")# 关键词来源sheet名称
    level:int = Field(...,ge=0,description="关键词层级")# 工作流层级层级
    error_callback: Optional[Callable[..., Any]] = Field(
        None, exclude=True, description="错误信息回调函数"
    )

class SourceRules(BaseModel):
    """增强版规则模型（包含预处理、去重、空值过滤）"""

    data: List[str] = Field(
        ..., min_length=1, description="经过预处理、去重且非空的规则列表"
    )

    error_callback: Optional[Callable] = Field(
        None, exclude=True, description="错误信息回调函数"
    )

    @field_validator("data", mode='before')
    def processing_pipeline(cls, v: Any, info:ValidationInfo) -> List[str]:
        """处理流水线：类型转换 -> 预处理 -> 空值过滤 -> 保序去重"""
        try:
            # 类型安全转换

            if not isinstance(v, (list, tuple, set)):
                raise ValueError("输入必须是可迭代对象")

            # 保留原始数据

            raw_rules = [str(item) for item in v]

            # 预处理流水线

            error_callback = info.data.get("error_callback")

            processed = [
                _preprocess_text(rule, error_callback).strip()  # 移除首尾空格
                for rule in raw_rules
            ]

            # 空值过滤（包括空白字符）

            non_empty = [rule for rule in processed if rule]

            # 保序去重逻辑

            return _preserve_order_deduplicate(non_empty)
        except Exception as e:
            logger.error(f"SourceRules的自定义数据校验出错：{e}")  # 记录错误信息到日志
            raise e

    class Config:
        validate_assignment = True


class WorkFlowRule(BaseModel):
    '''
    args:
        source_sheet_name:来源工作表名称
        rule:分类规则
        output_name:分类结果表名称
        classified_sheet_name:分类结果sheet名称
        parent_rule:父级规则
        level:工作流层级
    '''
    level:int = Field(...,ge=1,description="工作流层级")# 工作流层级
    source_sheet_name:str = Field(...,min_length=1,description="来源工作表名称")# 工作流来源工作表名称
    rule:str = Field(...,min_length=1,description="分类规则")# 分类规则
    output_name:str = Field(...,min_length=1,description="分类结果表名称")# 分类结果表名称
    classified_sheet_name:str|None = Field(None,min_length=1,description="分类结果sheet名称")# 分类结果sheet名称
    rule_tag:str|None = Field(None,min_length=1,description="规则标签")
    parent_rule:str|None = Field(None,min_length=1,description="父级规则")# 父级规则
    
    
    @model_validator(mode = 'after')
    def validate_rules(self)->'WorkFlowRule':
        """验证工作流规则"""
        err_msg = []
        if self.level > 1 and not self.classified_sheet_name:
            err_msg.append(f"工作流规则 {self.rule} 的流程层级大于1，但没有指定分类结果sheet名称,self is {self}")
        if self.level > 3 and not self.parent_rule:
            err_msg.append(f"工作流规则 {self.rule} 的流程层级为{self.level}，但指定没有指定父规则,self is {self}")
        if self.level >=3 and not self.rule_tag:
            err_msg.append(f"工作流规则 {self.rule} 的流程层级为{self.level}，但指定没有指定规则标签,self is {self}")
        if err_msg:
            raise ValueError("\n".join(err_msg))
        return self

class WorkFlowRules(BaseModel):
    '''
    args:
        rules:工作流规则列表
        workFlowRlue:
            args:
                source_sheet_name:来源工作表名称
                rule:分类规则
                output_name:分类结果表名称
                classified_sheet_name:分类结果sheet名称
                parent_rule:父级规则
                level:工作流层级
    '''
    rules:List[WorkFlowRule] = Field(...,min_length=1,description="工作流规则")# 工作流规则
    
    def __getitem__(self, key: str) -> Optional['WorkFlowRules']:
        """通过sheet名称获取对应的工作流规则列表"""
        rules = [rule for rule in self.rules if rule.source_sheet_name == key]
        if rules:
            return WorkFlowRules(rules=rules)
    
    def get_rules_by_level(self, level: int) -> Optional['WorkFlowRules']:
        """通过层级获取对应的工作流规则列表"""
        rules = [rule for rule in self.rules if rule.level == level]
        if rules:
            return WorkFlowRules(rules=rules)
    def get_parent_rules_name_by_level(self, level: int) -> List[str]:
        """获取指定层级的所有父规则"""
        parent_rules_name_list = [rule.parent_rule for rule in self.rules if rule.level == level and rule.parent_rule]
        return parent_rules_name_list

    def get_child_rules(self, parent_rule: str) -> Optional['WorkFlowRules']:
        """获取指定父规则的所有子规则"""
        rules = [rule for rule in self.rules if rule.parent_rule == parent_rule]
        if rules:
            return WorkFlowRules(rules=rules)
    def max_process_level(self)->int:
        """获取最大层级"""
        return max([rule.level for rule in self.rules])

    def filter_rules(self, **conditions: Any) -> Optional['WorkFlowRules']:
        """
        返回满足任意条件组合的 WorkFlowRule 列表。
        Conditions:
            source_sheet_name: 来源工作表名称

            rule: 分类规则

            output_name: 分类结果表名称

            classified_sheet_name: 分类结果sheet名称

            parent_rule: 父级规则

            rule_tag: 规则标签

            level: 工作流层级

        Args:
            **conditions: 条件字典，键为 WorkFlowRule 的字段名，值为期望的值或条件函数。
                         例如: `source_sheet_name="Sheet1"` 或 `level=lambda x: x > 2`
        
        Returns:
            WorkFlowRules: 满足条件的WorkFlowRules
        """
        filtered_rules = []
        for rule in self.rules:
            match = True
            for field, condition in conditions.items():
                if not hasattr(rule, field):
                    raise ValueError(f"Invalid field: '{field}' is not a valid field of WorkFlowRule")
                
                value = getattr(rule, field)
                

                # 如果条件是函数（如 lambda），则调用它进行判断
                if callable(condition):
                    if not condition(value):
                        match = False
                        break
                # 否则直接比较值
                elif value != condition:
                    match = False
                    break
            if match:
                filtered_rules.append(rule)
        return WorkFlowRules(rules=filtered_rules) if filtered_rules else None
    def to_rules_list(self)->List[str]:
        return [rule.rule for rule in self.rules]
    
    def get(
        self,
        key:str|int|None = None,
        source_sheet_name: Optional[str] = None,
        level: Optional[int] = None,
        parent_rule: Optional[str] = None,
        rule_tag: Optional[str] = None,
        **kwargs: Any
    ) -> Optional['WorkFlowRules']:
        """
        通用筛选方法，支持多种条件组合查询
        
        Args:
            key:根据key值查询，key可以是int类型，表示层级，也可以是str类型，表示来源工作表名称
            source_sheet_name: 按来源工作表名称筛选
            level: 按工作流层级筛选
            parent_rule: 按父级规则筛选
            rule_tag: 按规则标签筛选
            **kwargs: 其他WorkFlowRule字段的筛选条件
            
        Returns:
            符合条件的WorkFlowRules实例，若无匹配则返回None
        """
        filtered_rules = self.rules.copy()
        if key is not None:
            if isinstance(key, int):
                level = key
            elif isinstance(key, str):
                source_sheet_name = key
            else:
                raise ValueError("Invalid key type. Key must be an integer or a string.")
        
        # 应用固定条件的筛选
        if source_sheet_name is not None:
            filtered_rules = [r for r in filtered_rules if r.source_sheet_name == source_sheet_name]
        
        if level is not None:
            filtered_rules = [r for r in filtered_rules if r.level == level]
            
        if parent_rule is not None:
            filtered_rules = [r for r in filtered_rules if r.parent_rule == parent_rule]
        
        if rule_tag is not None:
            filtered_rules = [r for r in filtered_rules if r.rule_tag == rule_tag]
        # 应用动态字段条件的筛选
        for field, value in kwargs.items():
            filtered_rules = [r for r in filtered_rules if getattr(r, field, None) == value]
        
        return WorkFlowRules(rules=filtered_rules) if filtered_rules else None
    
    @model_validator(mode = 'after')    
    def validate_rules(self)->'WorkFlowRules':
        """验证工作流规则"""
        err_msg = []
        check = []
        for rule in self.rules:
            if rule.rule_tag is None:
                rule_tag = ''
            else:
                rule_tag = rule.rule_tag
            check.append(f'{rule.output_name}-{rule.rule}-{rule.classified_sheet_name}-{rule_tag}')
        if len(check) != len(set(check)):
            count = [ check[i] for i, x in enumerate(check) if check.count(x) > 1]
            err_msg.append(f"工作流规则有重复{set(count)}")
        if err_msg:
            raise ValueError("\n".join(err_msg))
        return self


class ClassifiedKeyword(BaseModel):
    '''
    args:
        level:分类层级
        keyword:关键词
        matched_rule:匹配的规则
        output_name:输出文件名称
        classified_sheet_name:输出sheet名称
        rule_tag:规则标签
        parent_rule:父级规则
        parent_rule_column:父级规则列名
        rule_tag_column:规则标签列名
    '''
    level:int = Field(...,ge=1,description="分类层级")
    keyword:str = Field(...,min_length=1,description="关键词")
    matched_rule:str = Field(...,min_length=1,description="匹配的规则")
    output_name:str = Field(...,min_length=1,description="输出文件名称")
    classified_sheet_name:str|None = Field(None,min_length=1,description="输出sheet名称")
    parent_rule:str|None = Field(None,min_length=1,description="父级规则")
    rule_tag:str|None = Field(None,min_length=1,description="规则标签")
    parent_rule_column:str|None = Field(None,min_length=1,description="父级规则列名")
    rule_tag_column:str|None = Field(None,min_length=1,description="规则标签列名")

class UnMatchedKeyword(BaseModel):
    '''
    args:
        keyword:关键词
        output_name:输出文件名称
        classified_sheet_name:输出sheet名称
        level:分类层级
        parent_rule:父级规则
        rule_tag:规则标签
        parent_rule_columon:父级规则列名
        rule_tag_columon:规则标签列名
        source_sheet_name:来源工作表名称
    '''
    keyword:str = Field(...,min_length=1,description="关键词")
    output_name:str = Field(...,min_length=1,description="输出文件名称,默认未匹配关键词")
    classified_sheet_name:str = Field("未匹配关键词",min_length=1,description="输出sheet名称，默认Sheet1")
    level:int = Field(1,ge=1,description="分类层级")
    parent_rule:None = Field(None,description="容错，防止groupby报错")
    rule_tag:None = Field(None,description="容错，防止groupby报错")
    parent_rule_columon:None = Field(None,description="容错，防止groupby报错")
    rule_tag_columon:None = Field(None,description="容错，防止groupby报错")
    source_sheet_name:str = Field(...,min_length=1,description="来源工作表名称")

class ClassifiedResult(BaseModel):
    classified_keywords: List[ClassifiedKeyword] = Field(..., description="分类结果")
    unclassified_keywords: List[UnMatchedKeyword] = Field(..., description="未分类关键词")
    
    def group_by_output_name(self,match_type:Literal['match','unmatch']='match') -> dict[str, List[ClassifiedKeyword]]:
        """按输出文件名聚类"""
        result = {}
        data = None
        if match_type == 'match':
            data = self.classified_keywords
        elif match_type == 'unmatch':
            data = self.unclassified_keywords
        for keyword in data:
            if keyword.output_name not in result:
                result[keyword.output_name] = []
            result[keyword.output_name].append(keyword)
        return result
    
    def group_by_output_name_and_sheet(self,match_type:Literal['match','unmatch']='match') -> dict[tuple[str, str], List[ClassifiedKeyword]]:
        """按输出文件名和sheet名聚类"""
        result = {}
        data = None
        if match_type == 'match':
            data = self.classified_keywords
        elif match_type == 'unmatch':
            data = self.unclassified_keywords
        for keyword in data:
            key = (keyword.output_name, keyword.classified_sheet_name or "默认sheet")
            if key not in result:
                result[key] = []
            result[key].append(keyword)
        return result
    
    def group_by_output_name_sheet_and_parent(self,match_type:Literal['match','unmatch']='match') -> dict[tuple[str, str, str], List[ClassifiedKeyword]]:
        """按输出文件名、sheet名和父规则聚类"""
        result = {}
        data = None
        if match_type == 'match':
            data = self.classified_keywords
        elif match_type == 'unmatch':
            data = self.unclassified_keywords
        
        
        for keyword in data:
            key = (
                keyword.output_name, 
                keyword.classified_sheet_name or "默认sheet",
                keyword.parent_rule or "无父规则"
            )
            if key not in result:
                result[key] = []
            result[key].append(keyword)
        return result
    
    def get_grouped_keywords(self, group_by: Literal['output_name','sheet','parent_rule'] = "output_name",match_type:Literal['match','unmatch']='match') -> dict[str|tuple,List[ClassifiedKeyword|UnMatchedKeyword]]:
        """获取聚类结果
        
        Args:
            group_by: 聚类方式，可选值：
                - output_name: 按输出文件名聚类
                - sheet: 按输出文件名和sheet名聚类
                - parent_rule: 按输出文件名、sheet名和父规则聚类
        """
        group_methods = {
            "output_name": self.group_by_output_name,
            "sheet": self.group_by_output_name_and_sheet,
            "parent_rule": self.group_by_output_name_sheet_and_parent
        }
        
        if group_by not in group_methods:
            raise ValueError(f"不支持的聚类方式: {group_by}，支持的聚类方式: {list(group_methods.keys())}")
            
        return group_methods[group_by](match_type)
    
    
    def filter(
        self,
        *,
        classified_conditions: Optional[Dict[str, Any]] = None,
        unclassified_conditions: Optional[Dict[str, Any]] = None,
        require_all: bool = True
    ) -> 'ClassifiedResult':
        """
        根据条件筛选分类结果
        
        Args:
            classified_conditions: 分类关键词的筛选条件 (字段名: 期望值)
            unclassified_conditions: 未分类关键词的筛选条件
            require_all: 是否要求所有条件都满足 (AND 操作)，False 表示满足任一条件即可 (OR 操作)
            
        Returns:
            新的 ClassifiedResult 实例，包含筛选后的结果
        """
        def matches(item: BaseModel, conditions: Dict[str, Any]) -> bool:
            if not conditions:
                return True
                
            comparisons = []
            for field, expected in conditions.items():
                actual = getattr(item, field, None)
                if actual is not None and actual == expected:
                    comparisons.append(True)
                else:
                    comparisons.append(False)
            
            return all(comparisons) if require_all else any(comparisons)

        filtered_classified = [
            kw for kw in self.classified_keywords 
            if matches(kw, classified_conditions or {})
        ]
        
        filtered_unclassified = [
            kw for kw in self.unclassified_keywords 
            if matches(kw, unclassified_conditions or {})
        ]
        return ClassifiedResult(
            classified_keywords=filtered_classified,
            unclassified_keywords=filtered_unclassified
        )
    def is_empty(self,classified_type:Literal['classified_keywords','unclassified_keywords']) -> bool:
        """判断分类结果是否为空"""
        return not getattr(self,classified_type)

class ProcessFilePath(BaseModel):
    '''
    args:
        output_name:输出文件名称
        classified_sheet_name:输出sheet名称
        file_path:文件路径
    '''
    level:int = Field(...,ge=1,description="分类层级")# 分类层级
    output_name:str = Field(...,min_length=1,description="输出文件名称")# 输出文件名称
    file_path:Path = Field(...,description="文件路径")# 文件路径
    classified_sheet_name:str|None = Field(None,min_length=1,description="输出sheet名称")# 输出sheet名称
    

    @model_validator(mode = 'after')    
    def validate_classified_sheet_name(self):
        """验证分类sheet名称"""
        if self.classified_sheet_name is None and self.level > 1:
            raise ValueError(f"output_name:{self.output_name},file_path:{self.file_path},分类层级为{self.level}，但没有指定分类sheet名称")
        return self
    
class ProcessFilePaths(BaseModel):
    '''
    args:
        file_paths:文件路径列表
    '''
    file_paths:List[ProcessFilePath] = Field(...,min_length=1,description="分类结果列表")# 文件路径列表

    def filter(self,**conditions:Any)->'ProcessFilePaths':
        """
        返回满足任意条件组合的 ProcessFilePaths 列表。
        Conditions:
            output_name: 输出文件名称
            classified_sheet_name: 输出sheet名称
            level: 分类层级
        Args:
            **conditions: 条件字典，键为 ProcessFilePath 的字段名，值为期望的值或条件函数。
                         例如: `output_name="Sheet1"` 或 `level=lambda x: x > 2`
        Returns:
            ProcessFilePaths: 满足条件的ProcessFilePaths
        """
        filtered_file_paths = []
        for file_path in self.file_paths:
            match = True
            for field, condition in conditions.items():
                if not hasattr(file_path, field):
                    raise ValueError(f"Invalid field: '{field}' is not a valid field of ProcessFilePath")

                value = getattr(file_path, field)
                # 如果条件是函数（如 lambda），则调用它进行判断
                if callable(condition):
                    if not condition(value):
                        match = False
                        break
                # 否则直接比较值
                elif value != condition:
                    match = False
                    break
            if match:
                filtered_file_paths.append(file_path)
        return ProcessFilePaths(file_paths=filtered_file_paths)
    
    def empty(self)->bool:
        """判断是否为空"""
        return len(self.file_paths) == 0
    

class ProcessTempResult(BaseModel):
    '''
    流程的阶段结果

    args:

        level:int 阶段
        status:str 处理结果
        data:ClassifiedResult 处理结果
        message:str|None 错误信息

    '''
    level:int = Field(...,ge=1,description="分类层级")# 分类层级
    status:Literal["success", "fail", "warning"] = Field(...,description="处理结果")# 处理结果
    data:ClassifiedResult|None = Field(...,description="处理结果")# 处理结果
    message:str|None = Field(None,description="错误信息")# 错误信息
    
    @model_validator(mode = 'after')
    def validate_rules(self):
        """验证规则"""
        if self.status != "success" and (self.message is None or self.message == ""):
            raise ValueError(f"level:{self.level},status:{self.status},message:{self.message},请填写错误信息")
        return self

class ProcessLevelResult(BaseModel):
    '''
    流程的中间结果

    args:

        level:int 阶段
        status:Literal["success", "fail", "warning"]  处理结果
        next_level:int 下一个阶段名称,9999标识结束
        message:str|None 错误信息

    '''
    level:int = Field(...,ge=0,description="阶段")# 阶段
    status:Literal["success", "fail", "warning"] = Field(...,description="处理结果")# 处理结果
    next_level:int = Field(...,ge=0,description="下一个阶段名称")# 下一个阶段名称
    message:str|None = Field(None,description="错误信息")# 错误信息

    @model_validator(mode = 'after')
    def validate_rules(self):
        """验证规则"""
        if self.status == "fail" and (self.message is None or self.message == ""):
            raise ValueError("处理结果为fail时，必须提供错误信息")
        return self
