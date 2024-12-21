from pyhive import hive



conn = hive.Connection(host='localhost', port=10000, username='Xyy')




cursor = conn.cursor()



cursor.execute('show databases')




for result in cursor.fetchall():


    print(result)




conn.close()