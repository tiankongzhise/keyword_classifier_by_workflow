from src.kw_cf.excel_handler import ExcelHandler
from src.kw_cf.models import UnclassifiedKeywords, SourceRules
from src.kw_cf.keyword_classifier import KeywordClassifier
# from src.kw_cf.workflow_processor import WorkFlowProcessor
from src.kw_cf.workflow_processor_copy import WorkFlowProcessor
from pathlib import Path


class Test:
    def __init__(self,keyword_file: Path,
                 rule_file: Path,
                 output_dir: Path,
                 work_flowr_file:Path,
                 excel_handler: ExcelHandler|None =None,
                 keyword_classifier: KeywordClassifier|None =None
                 ):
        self.keyword_file = keyword_file
        self.rule_file = rule_file
        self.work_flowr_file = work_flowr_file
        self.output_dir = output_dir
        self.excel_handler = excel_handler or ExcelHandler()
        self.keyword_classifier = keyword_classifier or KeywordClassifier()
        self.keywords = None
        self.rules = None
        self.parsed_rules = None
        
    def test(self):
        
        self.keywords = UnclassifiedKeywords(data = self.excel_handler.read_keywords(self.keyword_file))
        
        self.rules = SourceRules(data = self.excel_handler.read_rules(self.rule_file))
        
        self.keyword_classifier.set_rules(self.rules)
        
        result = self.keyword_classifier.classify_keywords(self.keywords)
        
        for i in result:
            print(f'keyord:{i.keyword},matched_rule:{i.matched_rule}')

    def test_read_work_flow(self):
        result = self.excel_handler.read_workflow_rules(self.work_flowr_file)

        print(result)

        print(f'Sheet1:{result["Sheet1"]}')    
    
    def test_read_keywords(self):
        result = self.excel_handler.read_keyword_file(self.keyword_file)
        print(result)
        
    def test_workflow_processor(self):
        processor = WorkFlowProcessor()
        result = processor.process_workflow(self.work_flowr_file,self.keyword_file)
        return result
    

def main():
    test = Test(
        keyword_file=Path('data/待分类_百度提示词.xlsx'),
        rule_file=Path('data/分词规则.xlsx'),
        work_flowr_file=Path('data/工作流规则_1.xlsx'),
        output_dir=Path('result'),  
    )
    rsp = test.test_workflow_processor()
    print(f'rsp:{rsp}')
    
if __name__ == '__main__':
    main()
