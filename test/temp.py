from src.kw_cf_v2.excel_handler import read_keywords,read_work_flow_rules
from src.kw_cf_v2.models import FileInfo,SourceKeywordDTO
from pathlib import Path
from dataclasses import is_dataclass
from src.kw_cf_v2.utils import processing_keyword
from src.kw_cf_v2.main import main

def test_keyword_read():
    file_path = Path(__file__).parent.parent /"工作流结果1/网络工程_20250408101331..xlsx"
    print(f'file_path:{file_path}')
    print(f'file_path.exists():{file_path.exists()}')
    print(f'file_path.is_file():{file_path.is_file()}')
    file_info = FileInfo(file_path=file_path,sheet_name="Linux",file_name="网络工程")
    keywords = read_keywords(file_info)

def test_is_dataclass():
    print(is_dataclass(SourceKeywordDTO))

def test_dict_pop_none():
    x = {'a':'1'}
    y = x.pop('b')
    print(f'y:{y}')

def test_read_work_flow_rules():
    file_path = Path(__file__).parent.parent /"data/工作流规则_Java类分词.xlsx"
    print(f'file_path:{file_path}')
    print(f'file_path.exists():{file_path.exists()}')
    print(f'file_path.is_file():{file_path.is_file()}')
    file_info = FileInfo(file_path=file_path,sheet_name="Java类分词",file_name="Java类分词")
    rules = read_work_flow_rules(file_info)
    print(f'rules:{rules}')

def test_processing_keyword():
    keyword = ''
    print(f'keyword:{[keyword]}')
    print(f'processing_keyword(keyword):{[processing_keyword(keyword)]}')

def test_main():
    main()

if __name__ == '__main__':
    test_main()



