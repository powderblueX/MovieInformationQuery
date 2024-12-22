from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from pyhive import hive
import pymysql
from neo4j import GraphDatabase
import time
import json

app = Flask(__name__)
CORS(app, resources={r"/compare-query-stream": {"origins": "*"}})


# 配置 Hive 连接
HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_USERNAME = 'Xyy'

# 配置 MySQL 连接
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'xyy20040123'
MYSQL_DATABASE = 'MovieDB'

# 配置 Neo4j 连接
NEO4J_URI = "bolt://192.168.20.119:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "lmz20040429"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))


@app.route('/compare-query-stream', methods=['GET'])
def compare_query_stream():
    query_type = request.args.get('queryType', 'default')
    parameters = json.loads(request.args.get('parameters'))

    def stream_results():
        # 查询 MySQL
        yield from query_mysql(query_type, parameters)

        # 查询 Neo4j
        yield from query_neo4j(query_type, parameters)

        # 查询 Hive
        yield from query_hive(query_type, parameters)

    return Response(stream_results(), content_type='text/event-stream', headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})

    # def stream_results():
    #     yield "data: {}\n\n".format(json.dumps({"database": "MySQL", "time": 100, "queryResult": ["MySQL result"]}))
    #     time.sleep(1)
    #     yield "data: {}\n\n".format(json.dumps({"database": "Neo4j", "time": 200, "queryResult": ["Neo4j result"]}))
    #     time.sleep(1)
    #     yield "data: {}\n\n".format(json.dumps({"database": "Hive", "time": 300, "queryResult": ["Hive result"]}))
    # return Response(stream_results(), content_type='text/event-stream', headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


def query_mysql(query_type, parameters):
    try:
        start_time = time.time()

        db = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = db.cursor()
        if query_type == "time":
            time_type = parameters.get("timeType")
            year = parameters.get("year", 2020)
            month = parameters.get("month", 1)
            quarter = parameters.get("quarter", 1)
            if time_type == "year":
                query_mysql = f"""SELECT Year, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = {year} GROUP BY Year ORDER BY Year;"""
            elif time_type == "month":
                query_mysql = f"""SELECT Year, Month, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = {year} AND Month = {month} GROUP BY Year, Month
                                ORDER BY Year, Month;"""
            elif time_type == "quarter":
                query_mysql = f"""SELECT Year, 
                                    CASE 
                                    WHEN Month IN (1, 2, 3) THEN 1 
                                    WHEN Month IN (4, 5, 6) THEN 2 
                                    WHEN Month IN (7, 8, 9) THEN 3
                                    WHEN Month IN (10, 11, 12) THEN 4
                                    END AS Quarter, 
                                    COUNT(*) AS Movie_Count 
                                    FROM Movie 
                                    WHERE Year = {year}
                                    AND CASE 
                                    WHEN Month IN (1, 2, 3) THEN 1
                                    WHEN Month IN (4, 5, 6) THEN 2
                                    WHEN Month IN (7, 8, 9) THEN 3
                                    WHEN Month IN (10, 11, 12) THEN 4
                                    END = {quarter}
                                    GROUP BY Year, Quarter 
                                    ORDER BY Year, Quarter; """
            elif time_type == "weekday":
                query_mysql = f"""SELECT Year, Month, Day, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = 2004 AND Month = 1 AND Day = 20
                                GROUP BY Year, Month, Day
                                ORDER BY Year, Month, Day;"""
        elif query_type == "title":
            title = parameters.get("title", "")
            query_mysql = f"""SELECT F.Format 
                                FROM Movie M 
                                JOIN Movie_Format MF ON M.Movie_ID = MF.Movie_ID
                                JOIN Format F ON MF.Format_ID = F.Format_ID 
                                WHERE M.MovieTitle = '{title}';"""
        elif query_type == "director":
            director = parameters.get("director", "")
            query_mysql = f"""SELECT D.Director_Name, COUNT(*) AS Movie_Count
                            FROM Director D
                            JOIN direct_movie MD ON D.Director_ID = MD.Director_ID
                            GROUP BY D.Director_Name
                            HAVING D.Director_Name LIKE '%{director}%';"""
        elif query_type == "actor":
            actor_type = parameters.get("actorType")
            actor_name = parameters.get("actorName", "")
            if actor_type == "star":
                query_mysql = f"""SELECT A.Actor_Name, COUNT(*) AS Leading_Role_Count
                                FROM Actor A
                                JOIN Starring_Actor SA ON A.Actor_ID = SA.Actor_ID
                                GROUP BY A.Actor_Name
                                HAVING A.Actor_Name LIKE '%{actor_name}%';"""
            elif actor_type == "act":
                query_mysql = f"""SELECT A.Actor_Name, COUNT(*) AS Total_Role_Count
                                FROM Actor A
                                JOIN All_Actor AA ON A.Actor_ID = AA.Actor_ID
                                GROUP BY A.Actor_Name
                                HAVING A.Actor_Name LIKE '%{actor_name}%';"""
        elif query_type == "genre":
            genre = parameters.get("genre", "")
            query_mysql = f"""SELECT G.Genre, COUNT(*) AS Movie_Count
                            FROM Genre G
                            JOIN Movie_Genre MG ON G.Genre_ID = MG.Genre_ID
                            WHERE G.Genre = '{genre}'
                            GROUP BY G.Genre
                            ORDER BY Movie_Count DESC;"""
        elif query_type == "rating":
            rating_type = parameters.get("ratingType")
            score = parameters.get("score", 3)
            if rating_type == "score-above":
                query_mysql = f"""SELECT MovieTitle, Average_Score
                                FROM Movie
                                WHERE Average_Score > {score} ORDER BY Average_Score DESC;"""
            elif rating_type == "positive":
                query_mysql = f"""SELECT 
                                    Movie_ID, Review_ID, Score
                                FROM 
                                    Review_Partitioned
                                WHERE 
                                    Score = 5;"""
        elif query_type == "relation":
            relation = parameters.get("relation", "")
            if relation == "actors":
                query_mysql = f"""SELECT A1.Actor_Name AS Actor1, A2.Actor_Name AS Actor2, MV.Cooperation_Count
                                FROM Actor_Cooperation_MV MV
                                JOIN Actor A1 ON MV.Actor1_ID = A1.Actor_ID
                                JOIN Actor A2 ON MV.Actor2_ID = A2.Actor_ID
                                WHERE MV.Cooperation_Count > 24
                                ORDER BY MV.Cooperation_Count DESC;"""
            elif relation == "directors":
                query_mysql = f"""SELECT D.Director_Name, A.Actor_Name, C.Cooperation_Count
                                FROM Director_Actor_Cooperation C
                                JOIN Director D ON C.Director_ID = D.Director_ID
                                JOIN Actor A ON C.Actor_ID = A.Actor_ID
                                WHERE C.Cooperation_Count > 20
                                ORDER BY C.Cooperation_Count DESC;"""
            elif relation == "pair":
                query_mysql = f"""SELECT 
                                (SELECT Actor_Name FROM Actor WHERE Actor_ID = APCI.ActorID1) AS Actor1,
                                (SELECT Actor_Name FROM Actor WHERE Actor_ID = APCI.ActorID2) AS Actor2,
                                (SELECT Genre FROM Genre WHERE Genre_ID = APCI.Genre_ID) AS Genre,
                                APCI.Total_Comments
                            FROM ActorPair_GenreComments_ID AS APCI
                            WHERE APCI.Genre_ID = (SELECT Genre_ID FROM Genre WHERE BINARY Genre = 'Action')
                            ORDER BY APCI.Total_Comments DESC
                            LIMIT 1;"""
        elif query_type == "combine":
            combine_type = parameters.get("combineQuery")
            actor_name = parameters.get("actor-name-combine", "")
            genre = parameters.get("genre-combine", "")
            year = parameters.get("year-combine", 2020)
            if combine_type == "actor-movies-genre":
                query_mysql = f"""SELECT A.Actor_Name, G.Genre, COUNT(*) AS Movie_Count
                                FROM Actor A
                                JOIN Movie_Actor MA ON A.Actor_ID = MA.Actor_ID
                                JOIN Movie_Genre MG ON MA.Movie_ID = MG.Movie_ID
                                JOIN Genre G ON MG.Genre_ID = G.Genre_ID
                                WHERE A.Actor_Name LIKE '%{actor_name}%'
                                AND G.Genre = '{genre}'  
                                GROUP BY A.Actor_Name, G.Genre;"""
            elif combine_type == "actor-movies-year":
                query_mysql = f"""SELECT A.Actor_Name, COUNT(*) AS Movie_Count
                                FROM Actor A
                                JOIN Movie_Actor MA ON A.Actor_ID = MA.Actor_ID
                                JOIN Movie M ON MA.Movie_ID = M.Movie_ID
                                WHERE A.Actor_Name LIKE '%{actor_name}%'
                                AND M.Year = {year} GROUP BY A.Actor_Name;"""
        else:
            yield f"data: {json.dumps({'database': 'MySQL', 'time': None, 'queryResult': 'Invalid query'})}\n\n"
        
        cursor.execute(query_mysql)
        mysql_result = cursor.fetchall()
        
        yield f"data: {json.dumps({'database': 'MySQL', 'time': round((time.time() - start_time) * 1000), 'queryResult': mysql_result})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'database': 'MySQL', 'time': None, 'queryResult': str(e)})}\n\n"
    finally:
        if 'db' in locals():
            db.close()



def query_neo4j(query_type, parameters):
    try:
        start_time = time.time()
        session = driver.session()
        # 动态生成查询
        query_neo4j = ""
        if query_type == "time":
            time_type = parameters.get("timeType")
            year = parameters.get("year", 2020)
            month = parameters.get("month", 1)
            quarter = parameters.get("quarter", 1)
            if time_type == "year":
                query_neo4j = f"MATCH (m:Movies) WHERE m.Year = {year} RETURN count(m) AS movies_in_{year}"
            elif time_type == "month":
                query_neo4j = f"MATCH (m:Movies) WHERE m.Year = {year} AND m.Month = {month} RETURN count(m) AS movies_in_{year}_{month}"
            elif time_type == "quarter":
                start_month = 1 + (int(quarter) - 1) * 3
                end_month = start_month + 2
                query_neo4j = f"MATCH (m:Movies) WHERE m.Year = {year} AND m.Month >= {start_month} AND m.Month <= {end_month} RETURN count(m) AS movies_in_q{quarter}_{year}"
            elif time_type == "weekday":
                query_neo4j = "MATCH (m:Movies) WHERE m.Year = 2004 AND m.Month = 1 AND m.Day = 20 RETURN count(m) AS movies_on_tuesday_1_20_2004"
        elif query_type == "title":
            title = parameters.get("title", "")
            query_neo4j = f"MATCH (m:Movies)-[:HAS_FORMAT]->(f:Formats) WHERE m.Movie_Title = '{title}' RETURN count(DISTINCT f) AS versions_of_Movie"
        elif query_type == "director":
            director = parameters.get("director", "")
            query_neo4j = f"MATCH (d:Directors)-[:DIRECT]->(m:Movies) WHERE d.Director_Name = '{director}' RETURN count(m) AS movies_directed_by"
        elif query_type == "actor":
            actor_type = parameters.get("actorType")
            actor_name = parameters.get("actorName", "")
            if actor_type == "star":
                query_neo4j = f"MATCH (a:Actors)-[:STAR]->(m:Movies) WHERE a.Actor_Name = '{actor_name}' RETURN count(DISTINCT m) AS movies_stared_by"
            elif actor_type == "act":
                query_neo4j = f"MATCH (a:Actors)-[:ACT]->(m:Movies) WHERE a.Actor_Name = '{actor_name}' RETURN count(DISTINCT m) AS movies_acted_by"
        elif query_type == "genre":
            genre = parameters.get("genre", "")
            query_neo4j = f"MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres) WHERE g.Genre = '{genre}' RETURN count(m) AS {genre}_movies_count"
        elif query_type == "rating":
            rating_type = parameters.get("ratingType")
            score = parameters.get("score", 3)
            if rating_type == "score-above":
                query_neo4j = f"MATCH (m:Movies) WHERE m.Average_Score > {score} RETURN count(*)"
            elif rating_type == "positive":
                query_neo4j = f"MATCH (m:Movies)-[:HAS_REVIEW]->(r:Reviews) WHERE r.Score = 5 RETURN DISTINCT m.Movie_Title AS positive_review_movies"
        elif query_type == "relation":
            relation = parameters.get("relation", "")
            if relation == "actors":
                query_neo4j = "MATCH (a1:Actors)-[r:COOPERATED]-(a2:Actors) WHERE r.count > 24 RETURN a1.Actor_Name, a2.Actor_Name, r.count ORDER BY r.count DESC LIMIT 1"
            elif relation == "directors":
                query_neo4j = "MATCH (a:Actors)-[r:COOPERATED_WITH_DIRECTOR]->(d:Directors) WHERE r.count > 20 RETURN a.Actor_Name AS Actor, d.Director_Name AS Director, r.count AS Collaborations ORDER BY r.count DESC LIMIT 2"
            elif relation == "pair":
                query_neo4j = """MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres) WHERE g.Genre = 'Action' WITH m
                           MATCH (a1:Actors)-[:ACT]->(m)<-[:ACT]-(a2:Actors)
                           WHERE a1.Actor_ID < a2.Actor_ID
                           WITH a1, a2, sum(m.Reviews_Count) AS total_reviews
                           RETURN a1.Actor_Name AS Actor1, a2.Actor_Name AS Actor2, total_reviews
                           ORDER BY total_reviews DESC LIMIT 1"""
        elif query_type == "combine":
            combine_type = parameters.get("combineQuery")
            actor_name = parameters.get("actor-name-combine", "")
            genre = parameters.get("genre-combine", "")
            year = parameters.get("year-combine", 2020)
            if combine_type == "actor-movies-genre":
                query_neo4j = f"""MATCH (a:Actors {{Actor_Name: '{actor_name}'}})-[:ACT]->(m:Movies)-[:HAS_GENRE]->(g:Genres {{Genre: '{genre}'}}) RETURN a.Actor_Name AS Actor, g.Genre AS Genre, count(m) AS MovieCount;"""
            elif combine_type == "actor-movies-year":
                query_neo4j = f"""MATCH (a:Actors {{Actor_Name: '{actor_name}'}})-[:ACT]->(m:Movies {{Year: {year}}}) RETURN a.Actor_Name AS Actor, m.Year AS Year, count(m) AS MovieCount;"""
        else:
            yield f"data: {json.dumps({'database': 'Neo4j', 'time': None, 'queryResult': str(e)})}\n\n"
        # 执行查询
        neo4j_result = session.run(query_neo4j)
        formatted_result = [dict(record) for record in neo4j_result]
        yield f"data: {json.dumps({'database': 'Neo4j', 'time': round((time.time() - start_time) * 1000), 'queryResult': formatted_result})}\n\n"
    except Exception as e:
        error_message = f"Error while querying Neo4j: {str(e)}"
        yield f"data: {json.dumps({'database': 'Neo4j', 'time': None, 'queryResult': error_message})}\n\n"
    finally:
        session.close()



def query_hive(query_type, parameters):
    try:
        start_time = time.time()
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USERNAME)
        cursor = conn.cursor()
        if query_type == "time":
            time_type = parameters.get("timeType")
            year = parameters.get("year", 2020)
            month = parameters.get("month", 1)
            quarter = parameters.get("quarter", 1)
            if time_type == "year":
                query_hive = f"""SELECT Year, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = {year} GROUP BY Year ORDER BY Year"""
            elif time_type == "month":
                query_hive = f"""SELECT Year, Month, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = {year} AND Month = {month} GROUP BY Year, Month
                                ORDER BY Year, Month"""
            elif time_type == "quarter":
                query_hive = f"""SELECT Year, 
                                    CASE 
                                    WHEN Month IN (1, 2, 3) THEN 1 
                                    WHEN Month IN (4, 5, 6) THEN 2 
                                    WHEN Month IN (7, 8, 9) THEN 3
                                    WHEN Month IN (10, 11, 12) THEN 4
                                    END AS Quarter, 
                                    COUNT(*) AS Movie_Count 
                                    FROM Movie 
                                    WHERE Year = {year}
                                    AND CASE 
                                    WHEN Month IN (1, 2, 3) THEN 1
                                    WHEN Month IN (4, 5, 6) THEN 2
                                    WHEN Month IN (7, 8, 9) THEN 3
                                    WHEN Month IN (10, 11, 12) THEN 4
                                    END = {quarter}
                                    GROUP BY Year, Quarter 
                                    ORDER BY Year, Quarter"""
            elif time_type == "weekday":
                query_hive = f"""SELECT Year, Month, Day, COUNT(*) AS Movie_Count
                                FROM Movie
                                WHERE Year = 2004 AND Month = 1 AND Day = 20
                                GROUP BY Year, Month, Day
                                ORDER BY Year, Month, Day"""
        elif query_type == "title":
            title = parameters.get("title", "")
            query_hive = f"""SELECT F.Format 
                                FROM Movie M 
                                JOIN Movie_Format MF ON M.Movie_ID = MF.Movie_ID
                                JOIN Format F ON MF.Format_ID = F.Format_ID 
                                WHERE M.Movie_Title = '{title}'"""
        elif query_type == "director":
            director = parameters.get("director", "")
            query_hive = f"""SELECT D.Director_Name, COUNT(*) AS Movie_Count
                            FROM Director D
                            JOIN direct_movie MD ON D.Director_ID = MD.Director_ID
                            GROUP BY D.Director_Name
                            HAVING D.Director_Name LIKE '%{director}%'"""
        elif query_type == "actor":
            actor_type = parameters.get("actorType")
            actor_name = parameters.get("actorName", "")
            if actor_type == "star":
                query_hive = f"""SELECT A.Actor_Name, COUNT(*) AS Leading_Role_Count
                                FROM Actor A
                                JOIN Starring_Actor SA ON A.Actor_ID = SA.Actor_ID
                                GROUP BY A.Actor_Name
                                HAVING A.Actor_Name LIKE '%{actor_name}%'"""
            elif actor_type == "act":
                query_hive = f"""SELECT A.Actor_Name, COUNT(*) AS Total_Role_Count
                                FROM Actor A
                                JOIN All_Actor AA ON A.Actor_ID = AA.Actor_ID
                                GROUP BY A.Actor_Name
                                HAVING A.Actor_Name LIKE '%{actor_name}%'"""
        elif query_type == "genre":
            genre = parameters.get("genre", "")
            query_hive = f"""SELECT G.Genre, COUNT(*) AS Movie_Count
                            FROM Genre G
                            JOIN Movie_Genre MG ON G.Genre_ID = MG.Genre_ID
                            WHERE G.Genre = '{genre}'
                            GROUP BY G.Genre
                            ORDER BY Movie_Count DESC"""
        elif query_type == "rating":
            rating_type = parameters.get("ratingType")
            score = parameters.get("score", 3)
            if rating_type == "score-above":
                query_hive = f"""SELECT Movie_Title, Average_Score FROM Movie WHERE Average_Score > {score} ORDER BY Average_Score DESC"""
            elif rating_type == "positive":
                query_hive = f"""SELECT Movie_ID, Review_ID, Score FROM Review WHERE Score = 5"""
        elif query_type == "relation":
            relation = parameters.get("relation", "")
            if relation == "actors":
                query_hive = "SELECT A1.Actor_Name AS Actor1,A2.Actor_Name AS Actor2,COUNT(*) AS Cooperation_Count FROM All_Actor AA1 JOIN All_Actor AA2 ON AA1.Movie_ID = AA2.Movie_ID AND AA1.Actor_ID < AA2.Actor_ID JOIN Actor A1 ON AA1.Actor_ID = A1.Actor_ID JOIN Actor A2 ON AA2.Actor_ID = A2.Actor_ID GROUP BY A1.Actor_Name, A2.Actor_Name HAVING COUNT(*) > 24 ORDER BY Cooperation_Count DESC"
            elif relation == "directors":
                query_hive = "SELECT D.Director_Name, A.Actor_Name, T.Cooperation_Count FROM ( SELECT MD.Director_ID, AA.Actor_ID, COUNT(*) AS Cooperation_Count FROM Direct_Movie AS MD JOIN All_Actor AS AA ON MD.Movie_ID = AA.Movie_ID GROUP BY MD.Director_ID, AA.Actor_ID HAVING COUNT(*) > 20 ) AS T JOIN Director AS D ON T.Director_ID = D.Director_ID JOIN Actor AS A ON T.Actor_ID = A.Actor_ID ORDER BY T.Cooperation_Count DESC"
            elif relation == "pair":
                query_hive = """SELECT 
                                    A1.Actor_Name AS Actor1, 
                                    A2.Actor_Name AS Actor2, 
                                    SUM(M.Comment_Num) AS Total_Comments
                                FROM All_Actor AA1
                                JOIN All_Actor AA2 
                                    ON AA1.Movie_ID = AA2.Movie_ID 
                                    AND AA1.Actor_ID < AA2.Actor_ID
                                JOIN Actor A1 
                                    ON AA1.Actor_ID = A1.Actor_ID
                                JOIN Actor A2 
                                    ON AA2.Actor_ID = A2.Actor_ID
                                JOIN Movie M 
                                    ON AA1.Movie_ID = M.Movie_ID
                                JOIN Movie_Genre MG 
                                    ON M.Movie_ID = MG.Movie_ID
                                JOIN Genre G 
                                    ON MG.Genre_ID = G.Genre_ID
                                WHERE G.Genre = 'Action'
                                GROUP BY A1.Actor_Name, A2.Actor_Name
                                ORDER BY Total_Comments DESC
                                LIMIT 1"""
        elif query_type == "combine":
            combine_type = parameters.get("combineQuery")
            actor_name = parameters.get("actor-name-combine", "")
            genre = parameters.get("genre-combine", "")
            year = parameters.get("year-combine", 2020)
            if combine_type == "actor-movies-genre":
                query_mysql = f"""SELECT A.Actor_Name, G.Genre, COUNT(*) AS Movie_Count
                                FROM Actor A
                                JOIN Movie_Actor MA ON A.Actor_ID = MA.Actor_ID
                                JOIN Movie_Genre MG ON MA.Movie_ID = MG.Movie_ID
                                JOIN Genre G ON MG.Genre_ID = G.Genre_ID
                                WHERE A.Actor_Name LIKE '%{actor_name}%'
                                AND G.Genre = {genre}        
                                GROUP BY A.Actor_Name, G.Genre"""
            elif combine_type == "actor-movies-year":
                query_mysql = f"""SELECT A.Actor_Name, COUNT(*) AS Movie_Count
                                FROM Actor A
                                JOIN Movie_Actor MA ON A.Actor_ID = MA.Actor_ID
                                JOIN Movie M ON MA.Movie_ID = M.Movie_ID
                                WHERE A.Actor_Name LIKE '%{actor_name}%'
                                AND M.Year = {year} GROUP BY A.Actor_Name"""
        else:
            yield f"data: {json.dumps({'database': 'Hive', 'time': None, 'queryResult': str(e)})}\n\n"
        
        cursor.execute('USE MovieDB') 
        cursor.execute(query_hive)
        hive_result = cursor.fetchall()
        # 转换为 JSON 可序列化格式
        yield f"data: {json.dumps({'database': 'Hive', 'time': round((time.time() - start_time) * 1000), 'queryResult': hive_result})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'database': 'Hive', 'time': None, 'queryResult': str(e)})}\n\n"
    finally:
        if 'conn' in locals():
            conn.close()



if __name__ == '__main__':
    app.run(debug=True, port=5000)
