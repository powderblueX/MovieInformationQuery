import csv
import re

# 输入和输出文件路径
input_csv_path = "../src/8th_filter/final_utf.csv"  # 输入的 CSV 文件路径
output_csv_path = "../src/9th_filter/final.csv"  # 输出的 CSV 文件路径

def replace_numeric_entities(text):
    """将 &#数字; 或 &#数字 转换为对应的字符"""
    # 处理带分号的 &#数字;
    text = re.sub(r'&#(\d+);', lambda match: chr(int(match.group(1))), text)
    # 处理不带分号的 &#数字
    text = re.sub(r'&#(\d+)', lambda match: chr(int(match.group(1))), text)
    return text

def convert_csv(input_path, output_path):
    """将CSV文件中类似 &#34; 和 &#281 转换为正常符号"""
    try:
        with open(input_path, mode='r', encoding='utf-8') as infile, open(output_path, mode='w', encoding='utf-8', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            for row in reader:
                # 替换每一行的每个单元格
                converted_row = [replace_numeric_entities(cell) for cell in row]
                writer.writerow(converted_row)

        print(f"转换完成，结果已保存到 {output_path}")
    except Exception as e:
        print(f"处理文件时出错: {e}")

# 执行转换
convert_csv(input_csv_path, output_csv_path)
