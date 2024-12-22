# 第一次处理，选出TV数据

import pandas as pd
import json
import re

# 读取原始 CSV 文件
input_file = '../src/metadata/metadata_amazon.csv'  # 请将此路径替换为您的输入文件路径
df = pd.read_csv(input_file)

# 定义一个函数，去掉 Movie Title 中冒号及之后的部分，生成 Normal Title
def normalize_title(title):
    if ':' in title:  # 检查是否包含冒号
        return re.split(r':', title, 1)[0].strip()
    else:
        return None  # 没有冒号时返回 None

# 添加 Normal Title 列
df['Normal Title'] = df['Movie Title'].apply(normalize_title)

# 找到 Normal Title 出现超过 2 次的条目
title_counts = df['Normal Title'].dropna().value_counts()  # 排除空值
duplicate_titles = title_counts[title_counts > 2].index

# 筛选 Normal Title 在 duplicate_titles 中的行（新增 TV 判定条件）
additional_tv = df[df['Normal Title'].isin(duplicate_titles)]

# 基于现有条件的筛选
filtered_df = df[
    df['Movie Title'].str.contains('TV', na=False) |
    df['Movie Title'].str.contains('episode', case=False, na=False) |
    df['Genres'].str.contains('TV', na=False) |
    df['Genres'].str.contains('episode', case=False, na=False) |
    df['Movie Title'].str.contains('season 1', case=False, na=False) |
    df['Movie Title'].str.contains('season 2', case=False, na=False) |
    df['Movie Title'].str.contains('season 3', case=False, na=False) |
    df['Movie Title'].str.contains('season 4', case=False, na=False) |
    df['Movie Title'].str.contains('season 5', case=False, na=False) |
    df['Movie Title'].str.contains('season 6', case=False, na=False) |
    df['Movie Title'].str.contains('season 7', case=False, na=False) |
    df['Movie Title'].str.contains('season 8', case=False, na=False) |
    df['Movie Title'].str.contains('season 9', case=False, na=False) |
    df['Movie Title'].str.contains('season 10', case=False, na=False) |
    df['Movie Title'].str.contains('season 11', case=False, na=False) |
    df['Movie Title'].str.contains('season 12', case=False, na=False) |
    df['Movie Title'].str.contains('season 13', case=False, na=False) |
    df['Movie Title'].str.contains('season 14', case=False, na=False) |
    df['Movie Title'].str.contains('series 1', case=False, na=False) |
    df['Movie Title'].str.contains('series 2', case=False, na=False) |
    df['Movie Title'].str.contains('series 3', case=False, na=False) |
    df['Movie Title'].str.contains('series 4', case=False, na=False) |
    df['Movie Title'].str.contains('series 5', case=False, na=False) |
    df['Movie Title'].str.contains('series 6', case=False, na=False) |
    df['Movie Title'].str.contains('series 7', case=False, na=False) |
    df['Movie Title'].str.contains('series 8', case=False, na=False) |
    df['Movie Title'].str.contains('series 9', case=False, na=False) |
    df['Movie Title'].str.contains('series 10', case=False, na=False) |
    df['Movie Title'].str.contains('Complete Series', case=False, na=False) |
    df['Movie Title'].str.contains('set 1', case=False, na=False) |
    df['Movie Title'].str.contains('set 2', case=False, na=False) |
    df['Movie Title'].str.contains('set 3', case=False, na=False) |
    df['Movie Title'].str.contains('set 4', case=False, na=False) |
    df['Movie Title'].str.contains('set 5', case=False, na=False) |
    df['Movie Title'].str.contains('set 6', case=False, na=False) |
    df['Movie Title'].str.contains('set 7', case=False, na=False) |
    df['Movie Title'].str.contains('set 8', case=False, na=False) |
    df['Movie Title'].str.contains('set 9', case=False, na=False) |
    df['Movie Title'].str.contains(r'vol\.', case=False, na=False) |
    df['Movie Title'].str.contains('volume ', case=False, na=False) |
    df['Movie Title'].str.contains('Collection ', case=False, na=False) |
    df['Movie Title'].str.contains('NCAA', na=False) |
    df['Movie Title'].str.contains('wnba ', case=False, na=False) |
    df['Movie Title'].str.contains(r'v\.', case=False, na=False)|
    df['Movie Title'].str.contains('Vol ', case=False, na=False)
]

# 合并新增的 TV 判定条件
filtered_df = pd.concat([filtered_df, additional_tv]).drop_duplicates()

# 保存满足条件的记录到 TV.csv
tv_csv_file = '../src/1st_filter/TV.csv'  # 替换为您希望保存的 TV 数据文件路径
filtered_df.to_csv(tv_csv_file, index=False)

# 从原始数据中移除这些行
remaining_df = df.drop(filtered_df.index)
remaining_csv_file = '../src/1st_filter/Movie.csv'  # 替换为您希望保存的剩余数据文件路径
remaining_df.to_csv(remaining_csv_file, index=False)

print(f"筛选完成，共找到 {len(filtered_df)} 条满足条件的记录，已保存到 {tv_csv_file}")
print(f"剩余 {len(remaining_df)} 条记录，已保存到 {remaining_csv_file}")


# 读取 CSV 文件中的 'productID' 列
csv_file_path = '../src/1st_filter/TV_productIDs.csv'  # 替换为您的 CSV 文件路径
df = pd.read_csv(csv_file_path)
product_ids = set(df['Productid'].astype(str))  # 将 'productID' 转换为字符串并存入集合

# 读取原始 JSON 文件并筛选
input_json_file_path = '../src/metadata/Movies_and_TV_review.json'  # 替换为您的输入 JSON 文件路径
output_json_file_path = '../src/1st_filter/TV.json'  # 替换为您希望保存的输出 JSON 文件路径
updated_json_file_path = '../src/1st_filter/Movies_and_TV_updated.json'  # 替换为原始 JSON 文件的更新路径

with open(input_json_file_path, 'r', encoding='utf-8') as json_file, \
        open(output_json_file_path, 'w', encoding='utf-8') as output_json_file, \
        open(updated_json_file_path, 'w', encoding='utf-8') as updated_json_file:
    for i, line in enumerate(json_file, start=1):
        try:
            record = json.loads(line.strip())
            if str(record.get('asin')) in product_ids:
                # 直接写入筛选文件
                json.dump(record, output_json_file, ensure_ascii=False)
                output_json_file.write('\n')
            else:
                # 直接写入保留文件
                json.dump(record, updated_json_file, ensure_ascii=False)
                updated_json_file.write('\n')
        except json.JSONDecodeError as e:
            print(f"解析错误：{e}，在行：{i}")
        # 每处理 1 万行输出进度
        if i % 100000 == 0:
            print(f"已处理 {i} 行数据")
    print(f"数据处理完成，共处理 {i} 行")
