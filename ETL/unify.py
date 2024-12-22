import csv
import json
import re

# 定义文件路径
input_csv_path = "../src/8th_filter/formatted_output_utf.csv"  # 输入的文件路径
output_all_actors_json = "../src/8th_filter/invalid_all_actors_format.json"  # 不符合 All Actors 格式的行保存路径
output_starring_actors_json = "../src/8th_filter/invalid_starring_actors_format.json"  # 不符合 Starring Actors 格式的行保存路径
output_directors_json = "../src/8th_filter/invalid_directors_format.json"  # 不符合 Directors 格式的行保存路径
output_genres_json = "../src/8th_filter/invalid_genres_format.json"  # 不符合 Genres 格式的行保存路径

# 正则表达式：优化后的正则
valid_format_regex = re.compile(r'^[^,]+(?:,[^,]+)*$')  # 通用名字列表格式
valid_genres_regex = re.compile(r'^[A-Za-z\-\' ]+(?:, [A-Za-z\-\' ]+)*$')  # Genres 格式：支持多单词、连字符和撇号组合

# 初始化不符合格式的记录列表
invalid_all_actors = []
invalid_starring_actors = []
invalid_directors = []
invalid_genres = []

# 打开文件逐行读取
with open(input_csv_path, 'r', encoding='utf-8') as csv_file:
    reader = csv.DictReader(csv_file)
    for index, row in enumerate(reader):
        all_actors = row.get('All Actors', '')
        starring_actors = row.get('Starring Actors', '')
        directors = row.get('Directors', '')
        genres = row.get('Genres', '')

        # 检查 All Actors 字段是否符合格式
        if all_actors and not valid_format_regex.match(all_actors):
            invalid_all_actors.append({
                "Productid": row.get('Productid', 'Unknown'),
                "All Actors": all_actors
            })

        # 检查 Starring Actors 字段是否符合格式
        if starring_actors and not valid_format_regex.match(starring_actors):
            invalid_starring_actors.append({
                "Productid": row.get('Productid', 'Unknown'),
                "Starring Actors": starring_actors
            })

        # 检查 Directors 字段是否符合格式
        if directors and not valid_format_regex.match(directors):
            invalid_directors.append({
                "Productid": row.get('Productid', 'Unknown'),
                "Directors": directors
            })

        # 检查 Genres 字段是否符合格式
        if genres and not valid_genres_regex.match(genres):
            invalid_genres.append({
                "Productid": row.get('Productid', 'Unknown'),
                "Genres": genres
            })

        # 每处理 1000 行输出进度
        if (index + 1) % 1000 == 0:
            print(f"已处理 {index + 1} 行...")

# 保存不符合格式的记录到 JSON 文件
with open(output_all_actors_json, 'w', encoding='utf-8') as json_file:
    json.dump(invalid_all_actors, json_file, ensure_ascii=False, indent=4)

with open(output_starring_actors_json, 'w', encoding='utf-8') as json_file:
    json.dump(invalid_starring_actors, json_file, ensure_ascii=False, indent=4)

with open(output_directors_json, 'w', encoding='utf-8') as json_file:
    json.dump(invalid_directors, json_file, ensure_ascii=False, indent=4)

with open(output_genres_json, 'w', encoding='utf-8') as json_file:
    json.dump(invalid_genres, json_file, ensure_ascii=False, indent=4)

print(f"处理完成，不符合 All Actors 格式的记录已保存到 {output_all_actors_json}")
print(f"处理完成，不符合 Starring Actors 格式的记录已保存到 {output_starring_actors_json}")
print(f"处理完成，不符合 Directors 格式的记录已保存到 {output_directors_json}")
print(f"处理完成，不符合 Genres 格式的记录已保存到 {output_genres_json}")