import pandas as pd
import json

# 定义文件路径
input_csv_path = "../src/5th_filter/output.csv"  # 输入的文件路径
output_csv_path = "../src/6th_filter/output.csv"  # 输出的文件路径
log_json_path = "../src/6th_filter/fill_log.json"  # 填充记录日志的路径

# 读取文件
df = pd.read_csv(input_csv_path)

# 初始化填充记录
fill_log = []

# 遍历 CSV 文件的每一行
for index, row in df.iterrows():
    starring_actors = row['Starring Actors']
    all_actors = row['All Actors']

    # 检查条件：
    if pd.notna(starring_actors) and pd.notna(all_actors):
        # 两个字段都不为空，跳过
        continue
    elif pd.isna(starring_actors) and pd.isna(all_actors):
        # 两个字段都为空，跳过
        continue
    elif pd.isna(starring_actors) and pd.notna(all_actors):
        # 如果 Starring Actors 为空而 All Actors 不为空
        df.at[index, 'Starring Actors'] = all_actors
        fill_log.append({
            "Productid": row['Productid'],
            "Updated Field": "Starring Actors",
            "Copied From": "All Actors"
        })
    elif pd.isna(all_actors) and pd.notna(starring_actors):
        # 如果 All Actors 为空而 Starring Actors 不为空
        df.at[index, 'All Actors'] = starring_actors
        fill_log.append({
            "Productid": row['Productid'],
            "Updated Field": "All Actors",
            "Copied From": "Starring Actors"
        })

# 保存结果到新的 CSV 文件
df.to_csv(output_csv_path, index=False)

# 保存填充记录到 JSON 文件
with open(log_json_path, "w", encoding="utf-8") as log_file:
    json.dump(fill_log, log_file, ensure_ascii=False, indent=4)

print(f"处理完成，结果已保存到 {output_csv_path}")
print(f"填充日志已保存到 {log_json_path}")
