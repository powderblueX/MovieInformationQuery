import csv
import re
import json

# 定义文件路径
input_csv_path = "../src/7th_filter/output.csv"  # 输入的文件路径
output_csv_path = "../src/8th_filter/formatted_output.csv"  # 格式化后的文件保存路径
removed_json_path = "../src/8th_filter/removed_rows.json"  # 删除行的 Productid 信息保存路径
invalid_genres_path = "../src/8th_filter/invalid_genres_format.json"  # 不合法 Genres 格式的 JSON 文件

# 正则表达式：匹配有效的名字列表格式和符号处理规则
valid_format_regex = re.compile(r'^[^,]+(?:,[^,]+)*$')
allowed_genres_regex = re.compile(r'[A-Za-z\'\-]+')  # 允许的字符：字母、撇号、连字符

# 定义清理函数
def clean_field(value):
    if not isinstance(value, str) or not value.strip():
        return None

    # 删除开头的多余逗号
    value = re.sub(r'^,+', '', value)

    # 删除多余的逗号和空格
    value = re.sub(r',+', ',', value)  # 合并多个逗号为一个
    value = re.sub(r',\s*$', '', value)  # 删除结尾多余的逗号

    # 如果内容不符合名字列表的格式且可能是描述信息，返回 None
    if not valid_format_regex.match(value):
        return None

    return value.strip()

def clean_genres(genres):
    if not isinstance(genres, str) or not genres.strip():
        return None

    # 替换除撇号和连字符外的符号为逗号
    genres = re.sub(r"[^A-Za-z\'\-, ]", ',', genres)

    # 合并重复的逗号
    genres = re.sub(r',+', ',', genres)

    # 去重并保留顺序
    unique_genres = []
    for genre in genres.split(','):
        genre = genre.strip()
        if genre and genre not in unique_genres:
            unique_genres.append(genre)

    return ', '.join(unique_genres)

# 初始化删除行的日志
removed_rows = []

# 打开文件逐行处理并格式化
with open(input_csv_path, 'r', encoding='utf-8') as csv_file:
    reader = csv.DictReader(csv_file)
    fieldnames = reader.fieldnames

    # 打开输出文件
    with open(output_csv_path, 'w', encoding='utf-8', newline='') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for index, row in enumerate(reader):
            # 格式化需要处理的字段
            all_actors = clean_field(row.get('All Actors', ''))
            starring_actors = clean_field(row.get('Starring Actors', ''))
            directors = clean_field(row.get('Directors', ''))
            genres = clean_genres(row.get('Genres', ''))

            # 如果任何一个字段是非人名信息，删除这一行并记录 Productid
            if all_actors is None or starring_actors is None or directors is None or genres is None:
                removed_rows.append({
                    "Productid": row.get('Productid', 'Unknown'),
                    "Row Index": index
                })
                continue

            # 更新清理后的字段
            row['All Actors'] = all_actors
            row['Starring Actors'] = starring_actors
            row['Directors'] = directors
            row['Genres'] = genres

            # 写入格式化后的行
            writer.writerow(row)

            # 每处理 1000 行输出进度
            if (index + 1) % 1000 == 0:
                print(f"已处理 {index + 1} 行...")

# 处理 invalid_genres_format.json 文件
with open(invalid_genres_path, 'r', encoding='utf-8') as json_file:
    invalid_genres = json.load(json_file)

# 更新并清理 Genres 字段
for record in invalid_genres:
    if 'Genres' in record:
        record['Genres'] = clean_genres(record['Genres'])

# 保存删除行的日志到 JSON 文件
with open(removed_json_path, 'w', encoding='utf-8') as json_file:
    json.dump(removed_rows, json_file, ensure_ascii=False, indent=4)

print(f"处理完成，格式化后的数据已保存到 {output_csv_path}")
print(f"删除的行信息已保存到 {removed_json_path}")