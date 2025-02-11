import pandas as pd

# 读取CSV文件
df = pd.read_csv('D:/Download/test.csv')

# 将所有空值替换为-1
df.fillna(-1, inplace=True)

# 保存清洗后的CSV文件
df.to_csv('D:/Download/test_filled.csv',encoding='utf-8-sig', index=False)