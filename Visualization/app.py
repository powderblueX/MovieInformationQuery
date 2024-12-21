from flask import Flask, request, jsonify
from pyhive import hive
import pymysql
from neo4j import GraphDatabase

app = Flask(__name__)

# 配置 Hive 连接
HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_USERNAME = 'Xyy'

# 配置 MySQL 连接
MYSQL_HOST = '100.80.153.205'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Tj123456'
MYSQL_DATABASE = 'MovieDB'

# 配置 Neo4j 连接
NEO4J_URI = "bolt://100.80.153.205:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "lmz20040429"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

@app.route('/hive')
def get_databases():
    try:
        # 连接 Hive
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USERNAME)
        cursor = conn.cursor()
        
        # 执行查询
        cursor.execute('USE MovieDB') 
        cursor.execute('show tables')
        result = [row[0] for row in cursor.fetchall()]  # 提取数据库名称
        
        return {"databases": result}  # 直接返回 JSON 格式数据
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/mysql')
def get_tables():
    try:
        # 连接 MySQL
        db = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = db.cursor()

        # 执行查询
        cursor.execute("SHOW TABLES;")
        result = [row[0] for row in cursor.fetchall()]  # 提取表名

        return {"tables": result}  # 直接返回 JSON 格式数据
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        if 'db' in locals() and db:
            db.close()

@app.route('/neo4j')
def query():
    session = driver.session()
    try:
        # 直接写查询语句
        query = """
        MATCH (m:Movies)
        WHERE m.Year = 2004
        RETURN count(m) AS movies_in_2004
        """
        
        # 执行查询
        result = session.run(query)
        
        # 提取查询结果
        data = [
            {key: (value if not isinstance(value, int) else int(value))
             for key, value in record.items()}
            for record in result
        ]
        
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        session.close()


@app.route('/shutdown', methods=['POST'])
def shutdown():
    driver.close()
    return jsonify({"message": "Neo4j driver closed"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5000)
