import pandas as pd
import datetime
from pathlib import Path
from .models import WorkFlowRule,WorkFlowRules,UnclassifiedKeywords
from typing import  Dict,Optional,Callable
from .logger_config import logger

class ExcelHandler:
    def __init__(self,error_callback:Optional[Callable]=None):
        self.error_callback:Optional[Callable] = error_callback
    def read_rules(self, file_path: Path):
        """从Excel文件中读取分词规则，并进行去重"""
        try:
            # 默认读取分词规则sheet的分词规则列
            df = pd.read_excel(file_path, sheet_name='分词规则')
            
            # 检查是否存在分词规则列
            if '分词规则' in df.columns:
                rules = df['分词规则'].dropna().astype(str).tolist()
            else:
                # 如果没有找到分词规则列，使用第一列
                rules = df.iloc[:, 0].dropna().astype(str).tolist()            
            return rules
        except Exception as e:
            raise Exception(f"读取规则文件失败: {str(e)}")
    
    def read_keywords(self, file_path: Path):
        """从Excel文件中读取关键词，并进行去重"""
        try:
            df = pd.read_excel(file_path)
            
            # 使用第一列作为关键词列
            keywords = df.iloc[:, 0].dropna().astype(str).tolist()
                        
            return keywords
        except Exception as e:
            raise Exception(f"读取关键词文件失败: {str(e)}")
    
    def save_results(self, result_df: pd.DataFrame, output_file: Path|None=None,sheet_name:str|None=None):
        """保存分类结果到Excel文件
        
        Args:
            result_df: 包含分类结果的DataFrame
            output_file: 输出文件路径，如果为None则使用默认路径
        """
        try:            
            # 如果没有指定输出文件路径，则使用默认路径
            if output_file is None:
                # 获取当前时间作为文件名的一部分
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                # 设置默认输出目录
                output_dir = Path('./默认输出结果')
                # 设置默认文件名
                output_file = output_dir / f'默认输出结果_{current_time}.xlsx'
            else:
                output_file = Path(output_file)
            
            # 确保输出目录存在
            try:
                # 获取目录路径
                output_dir = output_file.parent
                # 创建目录
                output_dir.mkdir(parents=True, exist_ok=True)
                logger.debug(f"已创建或确认输出目录: {output_dir.absolute()}")
            except PermissionError:
                # 权限错误时，使用用户目录作为备选
                user_dir = Path.home()
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = user_dir / f"关键词分类结果_{current_time}.xlsx"
                logger.warning(f"无法创建原目录，将保存到用户目录: {output_file}")
            except Exception as e:
                # 其他错误时，保存到当前目录
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = Path(f"关键词分类结果_{current_time}.xlsx")
                logger.error(f"创建目录时出错: {str(e)}，将保存到当前目录: {output_file}")
            
            # 保存到Excel
            result_df.to_excel(output_file, index=False, sheet_name=sheet_name)
            
            return output_file
        except Exception as e:
            raise Exception(f"保存结果失败: {str(e)}")


    def read_workflow_rules(self, file_path: Path) -> WorkFlowRules:
        """读取工作流规则文件
        
        Args:
            file_path: 工作流规则文件路径
            
        Returns:
            包含各个sheet规则的字典
        """
        try:
            # 检查文件名是否符合规范
            file_name = file_path.name
            if not file_name.startswith("工作流规则_"):
                raise ValueError(f"工作流规则文件名必须以'工作流规则_'开头，当前文件名: {file_name}")
            
            # 读取Excel文件的所有sheet
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            # 检查是否至少有Sheet1
            if 'Sheet1' not in sheet_names:
                raise ValueError("工作流规则文件必须包含Sheet1")
            rules_data = []
            # 遍历所有sheet，读取规则
            for i, sheet_name in enumerate(sheet_names):
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                # 检查sheet是否有数据
                if df.empty:
                    continue
                # 检查必需列
                required_cols = ['分类规则', '结果文件名称']
                if i > 0:  # Sheet2及以上需要额外的列
                    required_cols.append('分类sheet名称')
                if i > 1:  # Sheet3及以上需要分类标签
                    required_cols.append('分类标签')
                if i > 2:  # Sheet4及以上需要上层分类规则
                    required_cols.append('上层分类规则')
                
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"Sheet '{sheet_name}' 缺少必需列: {', '.join(missing_cols)}")
                # 读取规则数据
                for _, row in df.iterrows():
                    # 只添加非空规则
                    if not pd.notna(row['分类规则']) or not row['分类规则'].strip():
                        continue
                    rule_data ={
                        'source_sheet_name':sheet_name,
                        'rule':row['分类规则'],
                        'output_name':row['结果文件名称'], 
                        'level': i+1
                    }
                    # 添加额外的列
                    if i > 0 and '分类sheet名称' in df.columns:
                        rule_data['classified_sheet_name'] = row['分类sheet名称']
                    
                    if i > 2 and '上层分类规则' in df.columns:
                        rule_data['parent_rule'] = row['上层分类规则']
                    rules_data.append(WorkFlowRule(**rule_data))
                    rule_data = {}
            return WorkFlowRules(rules = rules_data)
        except Exception as e:
            raise Exception(f"读取工作流规则失败: {str(e)}")
    
    def read_keyword_file(self, file_path: Path) -> UnclassifiedKeywords:
        """读取待分类文件
        
        Args:
            file_path: 待分类文件路径
            
        Returns:
            UnclassifiedKeywords
        """
        try:
            # 检查文件名是否符合规范
            file_name = file_path.name
            if not file_name.startswith("待分类_"):
                raise ValueError(f"待分类文件名必须以'待分类_'开头，当前文件名: {file_name}")
            
            # 读取Excel文件
            df = pd.read_excel(file_path)
            
            # 检查是否包含关键词列
            if '关键词' not in df.columns:
                raise ValueError("待分类文件必须包含'关键词'列")
            
            # 清理数据
            keywords = df['关键词'].dropna().astype(str).tolist()
            return UnclassifiedKeywords(data=keywords,levle=0,error_callback=self.error_callback)
        except Exception as e:
            raise Exception(f"读取待分类文件失败: {str(e)}")
    def read_stage_results(self, file_path: Path) -> Dict[str,pd.DataFrame]:
        """读取分类结果文件
        
        Args:
            file_path: 分类结果文件路径
            
        Returns:
            Dict[str:pd.DataFrame]classified_sheet_name:pd.DataFrame
        """
        try:
            # 读取Excel文件的所有sheet
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            result = {}
            
            for sheet_name in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                if df.empty:
                    continue
                if '关键词' not in df.columns:
                    raise ValueError(f"Sheet '{sheet_name}' 必须包含'关键词'列")
                result[sheet_name] = df
            return result
        except Exception as e:
            raise Exception(f"读取分类结果文件失败: {str(e)}")
    
    def read_stage_classified_sheet_name(self, file_path: Dict[str,Path]) -> Dict[str,Dict[str,list[str]|Path]]:
        """读取分类结果文件
        
        Args:
            file_path: 分类结果文件路径
            
        Returns:
           Dict[str,Dict[str,list[str]|Path]]:Dict[output_name,Dict[file_path,classified_sheet_name]]
        """
        try:
            result = {}
            for output_name,file_path in file_path.items():
                # 读取Excel文件的所有sheet
                excel_file = pd.ExcelFile(file_path)
                sheet_names = list(excel_file.sheet_names)
                result[output_name] = {'file_path':file_path, 'classified_sheet_name':sheet_names}
            return result
        except Exception as e:
            raise Exception(f"读取分类结果文件失败: {str(e)}")
