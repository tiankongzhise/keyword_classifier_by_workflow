from dataclasses import dataclass,is_dataclass,replace,field
from typing import List, Callable, Any, Dict,Set
from pathlib import Path
from copy import deepcopy
import tomllib

@dataclass
class SourceKeyword:
    '''
    待分类的关键词
    Attributes:
        keyword:str = field(default='')
        source_file_name:str = field(default='')
        source_sheet_name:str = field(default='')
        matched_info:dict = field(default_factory=dict)
    '''
    keyword:str = field(default='')
    source_file_name:str = field(default='')
    source_sheet_name:str = field(default='')
    source_file_Path:Path|None = field(default=None)
    matched_info:dict = field(default_factory=dict)

@dataclass
class ClassifiedKeyword:
    '''
    已经分类的关键词
    Attributes:
        keyword:str = field(default='')
        source_file_name:str = field(default='')
        source_sheet_name:str = field(default='')
        output_file_name:str = field(default='')
        output_sheet_name:str = field(default='')
        matched_rule:dict = field(default_factory=dict)
    '''
    keyword:str = field(default='')
    source_file_name:str = field(default='')
    source_sheet_name:str = field(default='')
    source_file_Path:Path|None = field(default=None)
    output_file_name:str = field(default='')
    output_sheet_name:str = field(default='')
    process_level:int = field(default=-1)
    matched_info:dict = field(default_factory=dict)

@dataclass
class WorkFlowRule:
    '''
    工作流规则
    Attributes:
        rule:str = field(default='')
        rule_level:int = field(default=-1)
        target_file_name:str = field(default='')
        target_sheet_name:str = field(default='')
        limit_last_matched_rule:str = field(default='')
        rule_tag:str = field(default='')
    '''
    
    rule:str = field(default='')
    rule_level:int = field(default=-1)
    target_file_name:str = field(default='')
    target_sheet_name:str = field(default='')
    limit_last_matched_rule:str = field(default='')
    rule_tag:str = field(default='')

@dataclass
class FileInfo:
    '''
    文件路径信息
    Attributes:
        file_name:str = field(default='')
        sheet_name:str = field(default='')
        file_path:Path = field(default_factory=Path)
    '''
    
    file_name:str = field(default='')
    sheet_name:str = field(default='')
    file_path:Path = field(default_factory=Path)

class FilterMixin:
    _case_sensitive:bool = False
    def __init_subclass__(cls,**kwargs):
        """验证子类是否为数据类"""
        super().__init_subclass__(**kwargs)
        config_path = Path(__file__).parent / "config.toml"
        with config_path.open("rb") as f:  # 必须用二进制模式
            config = tomllib.load(f)
        cls._case_sensitive = config.get("filter", {}).get("case_sensitive", False)

    def filter(self, **conditions:Dict[str, Any | Callable]):
        """检查实例是否满足所有条件"""
        filtered_item = []
        if not hasattr(self, "data"):
            raise AttributeError(f"类 {self.__class__.__name__} 必须包含一个名为 'data' 的属性")
        
        for item in self.data: # type:ignore
            match = True
            
            for field_name, condition in conditions.items():
                
                # 验证字段是否存在
                if not hasattr(item, field_name):
                    raise ValueError(f"字段 '{field_name}' 不存在于类 {item.__class__.__name__}")
                
                # 获取字段值并匹配条件
                value = getattr(item, field_name)
                
                if callable(condition):
                    if not condition(value):
                        match = False
                        break
                if isinstance(condition, str) and not self._case_sensitive:
                    if value.lower() != condition.lower():
                        match = False
                        break
                elif value != condition:
                    match = False
                    break
                if match:
                    filtered_item.append(item)
        return replace(self, data=deepcopy(filtered_item)) # type:ignore


class FieldColValueListMixin:
    _case_sensitive:bool = False
    def __init_subclass__(cls,**kwargs):
        """验证子类是否为数据类"""
        super().__init_subclass__(**kwargs)
        config_path = Path(__file__).parent / "config.toml"
        with config_path.open("rb") as f:  # 必须用二进制模式
            config = tomllib.load(f)
            cls._case_sensitive = config.get("filter", {}).get("case_sensitive", False)
    def value_set_by_field_name(self,field_name:str)->Set[Any]:
        """根据字段名获取字段值"""
        if not hasattr(self, "data"):
            raise AttributeError(f"类 {self.__class__.__name__} 必须包含一个名为 'data' 的属性")
        temp_list = []
        for item in self.data: # type:ignore
            if not hasattr(item, field_name):
                raise ValueError(f"字段 '{field_name}' 不存在于类 {item.__class__.__name__}")
            temp_list.append(getattr(item, field_name))
        return set(temp_list)

    
@dataclass
class SourceKeywordDTO(FilterMixin,FieldColValueListMixin):
    '''
    待分类关键词集合
    '''
    data:List[SourceKeyword] = field(default_factory=list)
    
@dataclass
class ClassifiedKeywordDTO(FilterMixin,FieldColValueListMixin):
    '''
    已经分类的关键词集合
    '''
    data:List[ClassifiedKeyword] = field(default_factory=list)
    
@dataclass
class WorkFlowRuleDTO(FilterMixin,FieldColValueListMixin):
    '''
    工作流规则集合
    '''
    data:List[WorkFlowRule] = field(default_factory=list)
    max_level:int = field(default=-1)

@dataclass
class FileInfoDTO(FilterMixin,FieldColValueListMixin):
    '''
    文件路径信息集合
    '''
    data:List[FileInfo]
