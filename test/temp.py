from pydantic import BaseModel
import pandas as pd

class Test(BaseModel):
    x:str

class TestList(BaseModel):
    y:list[Test]


def test():
    t = TestList(y=[Test(x='1'),Test(x='2')])
    df = pd.DataFrame(item.model_dump()  for item in t.y)
    print(df)

if __name__ == '__main__':
    test()
