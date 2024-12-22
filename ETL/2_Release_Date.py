# 第二次处理，从评论中找到Release date

import pandas as pd
import json
from datetime import datetime
from collections import defaultdict



# 读取电影信息的 CSV 文件
movie_csv_path = '../src/1st_filter/Movie.csv'  # 请将此路径替换为您的实际文件路径
movies_df = pd.read_csv(movie_csv_path)

# 读取 JSON 文件路径
json_file_path = '../src/1st_filter/Movies_and_TV_updated.json'  # 请将此路径替换为您的实际文件路径

# 读取 TXT 文件路径
txt_file_path = '../src/metadata/movies_utf8.txt'  # 替换为 TXT 文件路径

# 提取需要的 Productid 列，过滤 Release Date 为空的记录
product_ids = set(movies_df[movies_df['Release Date'].isna()]['Productid'].astype(str))

# 用于存储 asin 对应的最早 unixReviewTime 及其来源
earliest_review_time = defaultdict(lambda: float('inf'))
bloodline_info = {}

# 分块处理 JSON 文件以减少内存占用并提高效率
chunk_size = 100000  # 每次处理 10 万行
line_count = 0

print("开始处理 JSON 文件...")

with open(json_file_path, 'r', encoding='utf-8') as json_file:
    for line in json_file:
        line_count += 1
        try:
            record = json.loads(line.strip())
            asin = str(record.get('asin'))
            unix_time = record.get('unixReviewTime', float('inf'))

            # 检查 asin 是否在目标列表中
            if asin in product_ids:
                # 更新最早的 unixReviewTime 记录
                if unix_time < earliest_review_time[asin]:
                    earliest_review_time[asin] = unix_time
                    # 保存血缘信息，记录来源
                    bloodline_info[asin] = {
                        "Source": "JSON",
                        "Source Line": line_count,
                        "Review Time": unix_time,
                        "Review Data": record,
                    }
        except json.JSONDecodeError:
            print(f"解析错误，跳过第 {line_count} 行")

        # 每处理 10 万行，输出进度信息
        if line_count % chunk_size == 0:
            print(f"已处理 {line_count} 行 JSON 数据...")

print(f"JSON 文件处理完成，总共处理了 {line_count} 行数据。")

# 处理 TXT 文件
print("开始处理 TXT 文件...")

with open(txt_file_path, 'r', encoding='utf-8', errors='ignore') as txt_file:  # 添加 errors='ignore' 处理文件编码问题
    current_block = []
    block_count = 0
    for line in txt_file:
        if line.strip():  # 非空行，属于当前块
            current_block.append(line.strip())
        else:  # 空行，处理当前块
            if current_block:
                block_count += 1
                try:
                    # 将当前块转换为字典格式
                    block_data = {}
                    for entry in current_block:
                        key, value = entry.split(': ', 1)
                        block_data[key.strip()] = value.strip()

                    # 提取 asin 和时间
                    asin = block_data.get('product/productId')
                    review_time = int(block_data.get('review/time', float('inf')))

                    # 检查 asin 是否在目标列表中
                    if asin in product_ids:
                        if review_time < earliest_review_time[asin]:
                            earliest_review_time[asin] = review_time
                            bloodline_info[asin] = {
                                "Source": "TXT",
                                "Source Block": block_count,
                                "Review Time": review_time,
                                "Review Data": block_data,
                            }
                except Exception as e:
                    print(f"解析 TXT 块出错，块号: {block_count}, 错误信息: {e}")

            current_block = []  # 重置块

        # 每处理 10 万个块输出进度
        if block_count % 100000 == 0 and block_count > 0:
            print(f"已处理 {block_count} 个块...")

# 处理最后一块（如果文件不以空行结尾）
if current_block:
    block_count += 1
    try:
        block_data = {}
        for entry in current_block:
            key, value = entry.split(': ', 1)
            block_data[key.strip()] = value.strip()

        asin = block_data.get('product/productId')
        review_time = int(block_data.get('review/time', float('inf')))

        if asin in product_ids:
            if review_time < earliest_review_time[asin]:
                earliest_review_time[asin] = review_time
                bloodline_info[asin] = {
                    "Source": "TXT",
                    "Source Block": block_count,
                    "Review Time": review_time,
                    "Review Data": block_data,
                }
    except Exception as e:
        print(f"解析 TXT 块出错，块号: {block_count}, 错误信息: {e}")

print(f"TXT 文件处理完成，共处理了 {block_count} 个块。")


# 遍历 movies_df，更新 Release Date 列
print("开始更新 Release Date 列...")
for index, row in movies_df.iterrows():
    if pd.isna(row['Release Date']):
        product_id = str(row['Productid'])
        if product_id in earliest_review_time:
            unix_time = earliest_review_time[product_id]
            release_date = datetime.utcfromtimestamp(unix_time).strftime('%d/%b/%Y')  # 转为标准日期格式
            movies_df.at[index, 'Release Date'] = release_date

# 将更新后的数据保存到新的 CSV 文件，保持原顺序
output_csv_path = '../src/2nd_filter/Updated_Movie.csv'  # 请将此路径替换为您希望保存的路径
movies_df.to_csv(output_csv_path, index=False)

# 保存血缘信息到单独的日志文件
bloodline_log_path = '../src/2nd_filter/Bloodline_Log.json'
with open(bloodline_log_path, 'w', encoding='utf-8') as log_file:
    json.dump(bloodline_info, log_file, ensure_ascii=False, indent=4)

print(f"更新完成，结果已保存到 {output_csv_path}")
print(f"血缘信息已保存到 {bloodline_log_path}")
