import pandas as pd
import json

# 定义文件路径
input_csv_path = "../src/6th_filter/output.csv"  # 输入的文件路径
output_csv_path = "../src/7th_filter/output.csv"  # 输出的文件路径
log_json_path = "../src/7th_filter/removed_rows_log.json"  # 删除行日志保存路径

# 读取文件
df = pd.read_csv(input_csv_path)

# 初始化删除日志
removed_rows_log = []

# 遍历每一行，检查是否有字段为空
rows_to_remove = []
for index, row in df.iterrows():
    if row.isnull().any():
        # 记录被删除行的 Productid
        removed_rows_log.append({
            "Productid": row['Productid'],
            "Row Index": index
        })
        # 标记该行需要删除
        rows_to_remove.append(index)

# 删除标记的行
df.drop(rows_to_remove, inplace=True)

# 保存删除后的数据到新的 CSV 文件
df.to_csv(output_csv_path, index=False)

# 保存删除行日志到 JSON 文件
with open(log_json_path, "w", encoding="utf-8") as log_file:
    json.dump(removed_rows_log, log_file, ensure_ascii=False, indent=4)

print(f"处理完成，删除的行已保存到日志文件 {log_json_path}，结果保存到 {output_csv_path}")
