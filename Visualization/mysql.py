
 
import pymysql
 
# 打开数据库连接
db = pymysql.connect(host='100.80.153.205',
                     user='root',
                     password='Tj123456',
                     database='MovieDB')

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
 
# 使用 execute()  方法执行 SQL 查询 
cursor.execute("Show Tables;")
 
# 使用 fetchall() 方法获取所有数据
data = cursor.fetchall()

# 遍历并打印所有数据
print("Database tables:")
for row in data:
    print(row)

 
# 关闭数据库连接
db.close()