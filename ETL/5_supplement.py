import pandas as pd
import json

# 定义文件路径
source_csv_path = "../src/SupplementaryInfo.csv"  # 原始文件路径
target_csv_path = "../src/Movies.csv"  # 要填充的目标文件路径
output_csv_path = "../src/5th_filter/output.csv"  # 填充后的结果保存路径
log_json_path = "../src/5th_filter/fill_log.json"  # 填充记录保存路径

# 读取文件
target_df = pd.read_csv(target_csv_path)
source_df = pd.read_csv(source_csv_path)

# 定义进度间隔
progress_interval = 1000  # 每处理 1000 行输出一次进度

# 初始化填充记录
fill_log = []

# 遍历目标文件的每一行
for index, row in target_df.iterrows():
    updated_fields = []  # 记录当前行被更新的字段

    # 检查 Genres、All Actors 和 Directors
    if pd.isna(row['Genres']) or pd.isna(row['All Actors']) or pd.isna(row['Directors']):
        # 获取当前行的 Productid
        product_id = row['Productid']

        # 在源文件中查找对应的 Productid
        source_row = source_df[source_df['Productid'] == product_id]

        if not source_row.empty:
            # 获取源文件中对应行的 Genres、All Actors 和 Directors
            source_genres = source_row.iloc[0]['Genres'] if not pd.isna(source_row.iloc[0]['Genres']) else None
            source_actors = source_row.iloc[0]['All Actors'] if not pd.isna(source_row.iloc[0]['All Actors']) else None
            source_directors = source_row.iloc[0]['Directors'] if not pd.isna(source_row.iloc[0]['Directors']) else None

            # 如果 Genres 为空且源文件中有值，则填充
            if pd.isna(row['Genres']) and source_genres:
                target_df.at[index, 'Genres'] = source_genres
                updated_fields.append("Genres")

            # 如果 All Actors 为空且源文件中有值，则填充
            if pd.isna(row['All Actors']) and source_actors:
                target_df.at[index, 'All Actors'] = source_actors
                updated_fields.append("All Actors")

            # 如果 Directors 为空且源文件中有值，则填充
            if pd.isna(row['Directors']) and source_directors:
                target_df.at[index, 'Directors'] = source_directors
                updated_fields.append("Directors")

    # 检查 All Actors
    if not pd.isna(row['All Actors']) and pd.isna(row['Starring Actors']):
        target_df.at[index, 'Starring Actors'] = row['All Actors']
        updated_fields.append("All Actors")

    # 如果有字段被更新，记录日志
    if updated_fields:
        fill_log.append({
            "Productid": row['Productid'],
            "Row": index,
            "Updated Fields": updated_fields
        })

    # 输出进度
    if (index + 1) % progress_interval == 0:
        print(f"已处理 {index + 1} 行，共 {len(target_df)} 行")

# 将 Formats 为空的行填充为 "DVD"
formats_updated_count = 0
for index, row in target_df.iterrows():
    if pd.isna(row['Formats']):
        target_df.at[index, 'Formats'] = "DVD"
        formats_updated_count += 1

print(f"已将 {formats_updated_count} 行的 Formats 填充为 'DVD'")

# 保留目标文件的所有其他字段，并保存填充后的结果
target_df.to_csv(output_csv_path, index=False)

# 保存填充记录到 JSON 文件
with open(log_json_path, "w", encoding="utf-8") as log_file:
    json.dump(fill_log, log_file, ensure_ascii=False, indent=4)

print(f"填充完成，所有字段保留，结果已保存到 {output_csv_path}")
print(f"填充日志已保存到 {log_json_path}")
