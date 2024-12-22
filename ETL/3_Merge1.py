# 合并认为相同的电影，合并依据：Movie Title完全相同，发行年份相同

import pandas as pd
import json

# 读取 CSV 文件
input_csv_path = "../src/2nd_filter/Updated_Movie.csv"  # 替换为您的 CSV 文件路径
print(f"开始读取 CSV 文件: {input_csv_path}")
df = pd.read_csv(input_csv_path)
print(f"CSV 文件读取完成，共有 {len(df)} 行数据。")

# 查看数据概况
print(df.head())  # 检查前几行数据
print(df.info())  # 检查字段类型和空值数量

# 处理 Release Date，提取年份
def extract_year(date):
    try:
        return pd.to_datetime(date, errors='coerce').year  # 设置错误处理为 'coerce'
    except Exception as e:
        print(f"日期解析错误: {date}, 错误信息: {e}")
        return None

print("开始提取年份...")
df['Release Year'] = df['Release Date'].apply(extract_year)
print("年份提取完成。")

# 合并逻辑：以 Movie Title 和 Release Year 为键，聚合其他字段，并记录血缘关系
def aggregate_rows(group):
    lineage = list(group.index)  # 记录原始行号
    product_ids = list(group['Productid'].dropna().unique())  # 记录来源 Productid
    first_product_id = product_ids[0] if product_ids else None  # 取第一个 Productid

    # 输出中间值：分组行数
    print(f"正在处理分组: {group.name}，包含 {len(group)} 行")

    # 去重合并逻辑
    aggregated_data = {
        "Productid": first_product_id,  # 保留第一个 Productid
        "Release Date": group['Release Date'].dropna().min(),  # 取最早的 Release Date
        "Genres": ", ".join(sorted(set(group['Genres'].dropna().str.strip()))),  # 去重、排序、合并
        "Directors": ", ".join(sorted(set(group['Directors'].dropna().str.strip()))),  # 去重、排序、合并
        "Starring Actors": ", ".join(sorted(set(group['Starring Actors'].dropna().str.strip()))),  # 去重、排序、合并
        "All Actors": ", ".join(sorted(set(group['All Actors'].dropna().str.strip()))),  # 去重、排序、合并
        "Formats": ", ".join(sorted(set(group['Formats'].dropna().str.strip()))),  # 去重、排序、合并
        "Source Lines": lineage,  # 记录来源行号
        "Source Productids": product_ids  # 记录来源 Productid
    }

    # 输出中间值：每个分组的聚合结果
    print(f"分组 {group.name} 聚合完成: {aggregated_data}")
    return pd.Series(aggregated_data)

# 添加分组进度输出
print("开始分组并进行聚合处理...")
grouped = df.groupby(["Movie Title", "Release Year"], dropna=False)
print(f"已完成分组，共有 {len(grouped)} 个分组。")

# 遍历分组并处理
result = grouped.apply(aggregate_rows).reset_index()
print("分组合并完成，开始生成血缘信息...")

# 血缘信息
bloodline = {}
for _, row in result.iterrows():
    key = f"{row['Movie Title']}|{row['Release Year']}|{row['Productid']}"  # 包含合并后的 Productid
    bloodline[key] = {
        "Source Lines": row["Source Lines"],
        "Source Productids": row["Source Productids"]
    }
    # 输出中间值：每条血缘信息
    print(f"血缘记录: {key} -> {bloodline[key]}")

# 移除临时的 Source Lines 和 Source Productids 字段
result = result.drop(columns=["Source Lines", "Source Productids"])

# 保存合并结果
output_csv_path = "../src/3rd_filter/aggregated_movies.csv"
result.to_csv(output_csv_path, index=False)
print(f"合并后的数据已保存到 {output_csv_path}")

# 保存血缘信息
bloodline_log_path = "../src/3rd_filter/bloodline_log.json"
with open(bloodline_log_path, "w", encoding="utf-8") as log_file:
    json.dump(bloodline, log_file, ensure_ascii=False, indent=4)
print(f"血缘信息已保存到 {bloodline_log_path}")
