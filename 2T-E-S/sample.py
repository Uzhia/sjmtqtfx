import pandas as pd

# 读取原始 CSV 文件
input_csv_file = "test0211.csv"  # 替换为你的原始 CSV 文件路径
df = pd.read_csv(input_csv_file)

# 随机抽取 5000 条数据
sampled_df = df.sample(n=5000, random_state=42)  # 设置 random_state 以确保结果可复现

# 保存抽取的数据到新的 CSV 文件
output_csv_file = "sampled_5000.csv"  # 新的 CSV 文件路径
sampled_df.to_csv(output_csv_file, index=False, encoding='utf-8-sig')
print(f"随机抽取的 5000 条数据已保存到 {output_csv_file}")