from flask import Flask, request, jsonify, render_template_string
import json
import io

app = Flask(__name__)

# 用于存储日志内容
log_output = io.StringIO()

# 自定义 print 函数，将内容打印到日志中
def log_message(message):
    log_output.write(message + "\n")
    print(message)

def load_json(file_name):
    """Load a JSON file and return its content."""
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log_message(f"Error loading {file_name}: {e}")
        return None

def find_entry_by_last_part(file_data, target_id):
    """
    Find the entry in file_data where the key ends with the target_id.
    """
    if isinstance(file_data, dict):
        for key, value in file_data.items():
            if key.split('|')[-1] == target_id:
                return value
    return None

def find_in_supplement(file_data, target_id):
    """
    Find the entry in 5_Supplement.json with the given Productid.
    """
    for entry in file_data:
        if entry.get("Productid") == target_id:
            return entry
    return None

def count_and_collect_source_productids(file_data, source_productids):
    """
    Recursively count all Source Productids in file_data and collect them.
    """
    count = 0
    visited = set()  # To avoid circular references
    collected_ids = set()

    def helper(product_id):
        nonlocal count
        if product_id in visited:  # Avoid visiting the same ID again
            return
        visited.add(product_id)

        entry = find_entry_by_last_part(file_data, product_id)
        if entry:
            productids = entry.get("Source Productids", [])
            count += len(productids)
            collected_ids.update(productids)  # Add the found IDs to the set
            for pid in productids:
                helper(pid)

    for product_id in source_productids:
        helper(product_id)

    return count, collected_ids

def find_earliest_block(file_data, collected_ids):
    """
    Find the block in file_data with the earliest unixReviewTime for the collected IDs.
    """
    earliest_time = float('inf')
    earliest_block = None

    for product_id in collected_ids:
        entry = file_data.get(product_id)
        if entry:
            review_data = entry.get("Review Data", {})
            unix_time = review_data.get("unixReviewTime")
            if unix_time and unix_time < earliest_time:
                earliest_time = unix_time
                earliest_block = entry

    return earliest_block

@app.route('/query', methods=['POST'])
def query():
    # 清空日志
    log_output.seek(0)
    log_output.truncate()

    # 获取 target_id
    data = request.get_json()
    target_id = data.get('target_id', '').strip()

    if not target_id:
        log_message("Invalid or missing target_id")
        return jsonify({"error": "Invalid or missing target_id", "logs": log_output.getvalue()}), 400

    # 加载 JSON 文件
    log_message("Loading files...")
    file_5 = load_json('Files/5_Supplement.json')
    file_4 = load_json('Files/4_merge.json')
    file_3 = load_json('Files/3_merge.json')
    file_2 = load_json('Files/2_date.json')

    if not all([file_5, file_4, file_3, file_2]):
        log_message("Failed to load one or more files.")
        return jsonify({"error": "Failed to load one or more files", "logs": log_output.getvalue()}), 500

    # 查询 4_merge.json
    log_message(f"Searching for ID: {target_id}")
    entry_4 = find_entry_by_last_part(file_4, target_id)
    if not entry_4:
        log_message(f"ID {target_id} not found in 4_merge.json.")
        return jsonify({"error": f"ID {target_id} not found in 4_merge.json", "logs": log_output.getvalue()}), 404

    log_message(f"Found in 4_merge.json: {entry_4}")

    # 获取 Source Productids
    source_productids = entry_4.get("Source Productids", [])
    if not source_productids:
        log_message(f"No Source Productids found for ID {target_id} in 4_merge.json.")
        return jsonify({"error": "No Source Productids found.", "logs": log_output.getvalue()}), 404

    log_message(f"Source Productids from 4_merge.json: {source_productids}")

    # 递归计算 Source Productids
    total_count, collected_ids = count_and_collect_source_productids(file_3, source_productids)
    log_message(f"共合并了: {total_count}个网页")
    log_message(f"网页名称: {list(collected_ids)}")

    # 查询最早的块
    earliest_block = find_earliest_block(file_2, collected_ids)
    if earliest_block:
        log_message(f"日期来自: {earliest_block.get('Source')}评论")
    else:
        log_message("日期来自Amazon")

    # 查询 5_Supplement.json
    supplement_entry = find_in_supplement(file_5, target_id)
    if supplement_entry:
        updated_fields = supplement_entry.get("Updated Fields", [])
        log_message(f"来着IMDB的字段: {updated_fields}")
    else:
        log_message(f"其余字段来自Amazon")

    return jsonify({"message": "Query completed", "logs": log_output.getvalue()}), 200

@app.route('/')
def home():
    # 提供查询界面
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>溯源查询</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; text-align: center; }
            input, button, textarea { margin: 10px 0; width: 100%; padding: 10px; }
            textarea { height: 300px; }
        </style>
    </head>
    <body>
        <h1>溯源查询</h1>
        <div>
            <input type="text" id="targetId" placeholder="请输入ID">
            <button onclick="query()">查询</button>
            <textarea id="logs" readonly></textarea>
        </div>
        <script>
            function query() {
                const targetId = document.getElementById('targetId').value.trim();
                if (!targetId) {
                    alert('请输入一个有效的ID');
                    return;
                }
                fetch('/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ target_id: targetId })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.error) alert(data.error);
                    document.getElementById('logs').value = data.logs || '无日志内容';
                })
                .catch(error => alert('查询失败: ' + error));
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html)

if __name__ == "__main__":
    app.run(debug=True)
