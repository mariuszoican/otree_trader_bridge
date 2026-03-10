import pandas as pd
import numpy.random as npr

npr.seed(1007)
dividends=npr.choice([0,4,8,20],60)
print(dividends)
df=pd.DataFrame(dividends,columns=['dividend_per_share'])
df.to_csv('dividends.csv',index=False)