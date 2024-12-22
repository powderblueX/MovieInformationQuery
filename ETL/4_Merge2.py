# 合并同一部电影的不同语言或格式的版本，合并依据：将Movie Title 中括号的内容去掉后比较，若相同则认为是一部电影

import pandas as pd
import re
import json

# 读取 CSV 文件
input_csv_path = "../src/3rd_filter/aggregated_movies.csv"  # 替换为您的 CSV 文件路径
output_csv_path = "../src/4th_filter/matched_movies.csv"  # 筛选结果保存的路径
bloodline_log_path = "../src/4th_filter/bloodline_log.json"  # 血缘信息保存路径

# 加载数据
df = pd.read_csv(input_csv_path)

# 定义一个函数，去掉 Movie Title 中括号及括号内的内容，并移除媒介标识
def normalize_title(title):
    # 去掉括号及括号内的内容
    title = re.sub(r"\s*\(.*?\)", "", title).strip()
    # 去掉 VHS、DVD 等媒介标识
    title = re.sub(r"\b(VHS|DVD|Blu-ray|4K|Digital)\b", "", title, flags=re.IGNORECASE).strip()
    return title

# 添加一个新列，用于存储去括号及媒介标识后的标题
df['Normalized Title'] = df['Movie Title'].apply(normalize_title)

# 按 `Normalized Title` 分组
grouped = df.groupby('Normalized Title')

# 初始化列表，用于存储合并后的结果
merged_rows = []
bloodline_info = {}  # 用于记录血缘信息

# 遍历每个分组
for title, group in grouped:
    # 获取分组中所有行号和 Productid
    source_lines = list(group.index)
    source_product_ids = list(group['Productid'])

    # 分组中有多个版本，进行合并
    productid = group.loc[group['Normalized Title'] == group['Movie Title'], 'Productid'].iloc[0] if \
        any(group['Normalized Title'] == group['Movie Title']) else group['Productid'].iloc[0]

    release_year = group['Release Year'].dropna().min()  # 取最早的 Release Year
    release_date = group['Release Date'].dropna().min()  # 取最早的 Release Date

    # 合并去重的字段
    genres = ", ".join(sorted(set(group['Genres'].dropna().str.strip())))
    directors = ", ".join(sorted(set(group['Directors'].dropna().str.strip())))
    starring_actors = ", ".join(sorted(set(group['Starring Actors'].dropna().str.strip())))
    all_actors = ", ".join(sorted(set(group['All Actors'].dropna().str.strip())))
    formats = ", ".join(sorted(set(group['Formats'].dropna().str.strip())))

    # 构造合并后的行
    merged_row = {
        "Movie Title": title,  # 使用去括号和媒介标识后的标题
        "Release Year": release_year,
        "Productid": productid,
        "Release Date": release_date,
        "Genres": genres,
        "Directors": directors,
        "Starring Actors": starring_actors,
        "All Actors": all_actors,
        "Formats": formats,
    }
    merged_rows.append(merged_row)

    # 记录血缘信息
    bloodline_info[productid] = {
        "Source Lines": source_lines,
        "Source Productids": source_product_ids,
    }

# 将结果保存为 DataFrame
result_df = pd.DataFrame(merged_rows)

# 再次去重处理 `Formats` 字段
if 'Formats' in result_df.columns:
    result_df['Formats'] = result_df['Formats'].apply(
        lambda x: ", ".join(sorted(set(map(str.strip, x.split(',')))))
    )

# 确保移除 `Normalized Title` 列（如果存在）
columns_to_remove = ["Normalized Title"]
for col in columns_to_remove:
    if col in result_df.columns:
        result_df = result_df.drop(columns=[col])

# 保存合并后的数据到 CSV 文件
result_df.to_csv(output_csv_path, index=False)
print(f"合并完成，结果已保存到 {output_csv_path}")

# 保存血缘信息到 JSON 文件
with open(bloodline_log_path, "w", encoding="utf-8") as log_file:
    json.dump(bloodline_info, log_file, ensure_ascii=False, indent=4)
print(f"血缘信息已保存到 {bloodline_log_path}")
