import csv

# 输入和输出文件路径
input_csv_path = "../src/9th_filter/final.csv"  # 输入的 CSV 文件路径
output_csv_path = "../src/9th_filter/filtered_rows_with_name.csv"  # 输出的 CSV 文件路径

def filter_rows_with_name_error(input_path, output_path):
    """筛选出所有字段包含 #NAME? 的行"""
    try:
        with open(input_path, mode='r', encoding='utf-8') as infile, open(output_path, mode='w', encoding='utf-8', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # 遍历每一行，检查是否包含 #NAME?
            for row in reader:
                if any('#NAME?' in cell for cell in row):
                    writer.writerow(row)  # 写入包含 #NAME? 的行

        print(f"筛选完成，包含 #NAME? 的行已保存到 {output_path}")
    except Exception as e:
        print(f"处理文件时出错: {e}")

# 执行筛选
filter_rows_with_name_error(input_csv_path, output_csv_path)
