import pandas as pd
import chardet

# 文件路径
normal_file = "../src/3rd_filter/aggregated_movies.csv"
problem_file = "../src/9th_filter/final_utf.csv"
output_file = "../src/10th_filter/FINAL.csv"

# 检测文件的编码
def detect_encoding(file_path, num_bytes=10000):
    with open(file_path, "rb") as f:
        raw_data = f.read(num_bytes)
        result = chardet.detect(raw_data)
    return result['encoding']

# 获取正常文件的列的编码
def get_column_encodings(file_path):
    encodings = {}
    with open(file_path, "rb") as f:
        for line in f.readlines():
            result = chardet.detect(line)
            encodings[line.decode(errors='ignore')] = result['encoding']
    return encodings

# 尝试修复混乱编码
def try_fix_encoding_with_all_encodings(text, encodings):
    for encoding in encodings:
        try:
            return text.encode(encoding).decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text  # 如果所有尝试都失败，返回原文本

# 按列尝试解码问题文件
def fix_problem_file(normal_file, problem_file, output_file):
    # 检测正常文件和问题文件的整体编码
    normal_encoding = detect_encoding(normal_file)
    problem_encoding = detect_encoding(problem_file)

    print(f"正常文件的编码检测结果: {normal_encoding}")
    print(f"问题文件的编码检测结果: {problem_encoding}")

    # 获取正常文件每列的编码
    column_encodings = get_column_encodings(normal_file)
    print(f"正常文件每列的编码: {column_encodings}")

    # 读取正常文件和问题文件
    df_normal = pd.read_csv(normal_file, encoding=normal_encoding, low_memory=False)
    df_problem = pd.read_csv(problem_file, encoding=problem_encoding, on_bad_lines='skip', low_memory=False)

    # 移除无效的列（如 Unnamed）
    df_normal = df_normal.loc[:, ~df_normal.columns.str.contains('^Unnamed')]
    df_problem = df_problem.loc[:, ~df_problem.columns.str.contains('^Unnamed')]

    # 修复数据
    df_fixed = df_problem.copy()
    for col in df_fixed.columns:
        if col in df_normal.columns:
            print(f"正在修复列: {col}")
            # 尝试用正常文件每列的编码修复问题文件的列
            encodings = list(column_encodings.values())
            df_fixed[col] = df_fixed[col].apply(lambda x: try_fix_encoding_with_all_encodings(str(x), encodings))

    # 保存修复后的文件
    df_fixed.to_csv(output_file, encoding="utf-8", index=False)
    print(f"修复完成，文件已保存为 {output_file}")

# 调用修复函数
try:
    fix_problem_file(normal_file, problem_file, output_file)
except Exception as e:
    print(f"处理文件时出错: {e}")
