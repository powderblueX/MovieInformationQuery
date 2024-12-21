# neo4j图数据库

## neo4j配置

> 详细配置见**neo4j.conf**文件中

## 建表语句

### Movies表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Movie_Table.csv' AS row
CREATE (:Movies {
  Movie_ID: toInteger(row.New_Productid),
  Movie_Title: trim(row.Movie_Title), 
  Average_Score: toFloat(row.Average_Score),
  Day: toInteger(row.Day),
  Month: toInteger(row.Month),
  Year: toInteger(row.Year),
  Reviews_Count: toInteger(row.Sample_Count)
})
```

### Actors表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Actor_Mapping_Table1.csv' AS row
CREATE (:Actors {
  Actor_ID: toInteger(row.New_Actor_ID),
  Actor_Name: trim(row.Actor_Name)
})
```

### Directors表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Directors_Mapping_Table.csv' AS row
CREATE (:Directors {
  Director_ID: toInteger(row.Director_ID_New),
  Director_Name: trim(row.Director_Name)
})
```

### Formats表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Format_Table.csv' AS row
CREATE (:Formats {
  Format_ID: trim(row.Format_ID),
  Format: trim(row.Format)
})
```

### Genres表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Genre_Table.csv' AS row
CREATE (:Genres {
  Genre_ID: trim(row.Genre_ID),
  Genre: trim(row.Genre)
})
```

### Reviews表

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Review_36.csv' AS row
CREATE (:Reviews {
  Review_ID: toInteger(row.reviewId),
  Score: toFloat(row.score)
})
```



## 建立关联关系语句

### HAS_FORMAT

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Movie_Format_Relationship.csv' AS row
MATCH (m:Movies {Movie_ID: toInteger(row.New_Movie_ID)})
MATCH (f:Formats {Format_ID: trim(row.Format_ID)})
MERGE (m)-[:HAS_FORMAT]->(f);
```

### HAS_GENRE

```cy
LOAD CSV WITH HEADERS FROM 'file:///Movie_Genre_Relationship.csv' AS row
MATCH (m:Movies {Movie_ID: toInteger(row.New_Movie_ID)})
MATCH (g:Genres {Genre_ID: trim(row.Genre_ID)})
MERGE (m)-[:HAS_GENRE]->(g);
```

### ACT

```cypher
LOAD CSV WITH HEADERS FROM 'file:///All_Actor_Relationship8.csv' AS row
MATCH (a:Actors {Actor_ID: toInteger(row.New_Actor_ID)})
MATCH (m:Movies {Movie_ID: toInteger(row.New_Productid)})
MERGE (a)-[:ACT]->(m);
```

### STAR

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Starring_Actor_Relationship.csv' AS row
MATCH (a:Actors {Actor_ID: toInteger(row.New_Actor_ID)})
MATCH (m:Movies {Movie_ID: toInteger(row.New_Productid)})
MERGE (a)-[:STAR]->(m);
```

### DIRECT

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Direct_Movie_Relationship.csv' AS row
MATCH (d:Directors {Director_ID: toInteger(row.Director_ID)})
MATCH (m:Movies {Movie_ID: toInteger(row.Movie_ID)})
MERGE (d)-[:DIRECT]->(m);
```

### HAS_REVIEW

```cypher
LOAD CSV WITH HEADERS FROM 'file:///Review.csv' AS row
MATCH (m:Movies {Movie_ID: toInteger(row.productId)})
MATCH (r:Reviews {Review_ID: toInteger(row.reviewId)})
MERGE (m)-[:HAS_REVIEW]->(r);
```



## 创建索引语句

### Movie_ID

```cypher
CREATE INDEX FOR (m:Movies) ON (m.Movie_ID);
```

### Format_ID

```cypher
CREATE INDEX FOR (f:Formats) ON (f.Format_ID);
```

### Genre_ID

```cy
CREATE INDEX FOR (g:Genres) ON (g.Genre_ID);
```

### Actor_ID

```cy
CREATE INDEX FOR (a:Actors) ON (a.Actor_ID);
```

### Review_ID

```cypher
CREATE INDEX FOR (r:Reviews) ON (r.Review_ID);
```

### Director_ID

```cypher
CREATE INDEX FOR (r:Directors) ON (r.Director_ID);
```



## 查询语句

### 按照时间进行查询及统计

1. XX年有多少电影

   ```cypher
   MATCH (m:Movies)
   WHERE m.Year = 2020
   RETURN count(m) AS movies_in_2020
   ```

2. XX年XX月有多少电影

   ```cypher
   MATCH (m:Movies)
   WHERE m.Year = 2020 AND m.Month = 5
   RETURN count(m) AS movies_in_may_2020
   ```

3. XX年XX季度有多少电影

   ```cypher
   MATCH (m:Movies)
   WHERE m.Year = 2020 AND m.Month >= 1 AND m.Month <= 3
   RETURN count(m) AS movies_in_q1_2020
   ```

4. 周二新增多少电影

   ```cypher
   MATCH (m:Movies)
   WHERE m.Year = 2004 AND m.Month = 1 AND m.Day = 20
   RETURN count(m) AS movies_in_Tues_2004
   ```


### 按照电影名称进行查询及统计

1. XX电影共有多少版本等

   ```cypher
   MATCH (m:Movies)-[:HAS_FORMAT]->(f:Formats)
   WHERE m.Movie_Title = "Marvel's The Avengers"
   RETURN count(DISTINCT f) AS versions_of_MarvelsTheAvengers
   ```


### 按照导演进行查询及统计

1. XX导演共有多少电影

   ```cypher
   MATCH (d:Directors)-[:DIRECT]->(m:Movies)
   WHERE d.Director_Name = "Christopher Nolan"
   RETURN count(m) AS movies_directed_by_Nolan
   ```


### 按照演员进行查询及统计

1. XX演员主演多少电影

   ```cypher
   MATCH (a:Actors)-[:STAR]->(m:Movies)
   WHERE a.Actor_Name = "Andrew Garfield"
   RETURN count(DISTINCT m) AS movies_stared_by_Andrew_Garfield
   ```

2. XX演员参演多少电影

   ```cypher
   MATCH (a:Actors)-[:ACT]->(m:Movies)
   WHERE a.Actor_Name = "Andrew Garfield"
   RETURN count(DISTINCT m) AS movies_stared_by_Andrew_Garfield
   ```


### 按照电影类别进行查询及统计

1. Action 电影共有多少

   ```cypher
   MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres)
   WHERE g.Genre = "Action"
   RETURN count(m) AS action_movies_count
   ```

2. Adventure 电影共有多少

   ```cypher
   MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres)
   WHERE g.Genre = "Adventure"
   RETURN count(m) AS action_movies_count
   ```


### 按照用户评价进行查询及统计

1. 用户评分3分以上的电影有哪些

   ```cypher
   MATCH (m:Movies)
   WHERE m.Average_Score > 3
   RETURN m.Movie_Title AS movies_reviews_above_3
   ```

2. 用户评价中有正面评价的电影有哪些

   ```cypher
   MATCH (m:Movies)-[:HAS_REVIEW]->(r:Reviews)
   WHERE r.Score = 5
   WITH m.Movie_Title AS title
   RETURN DISTINCT title
   ORDER BY title
   ```

   Started streaming 66563 records after 10 ms and completed after 1315 ms

### 按照演员、导演之间的关系进行查询及统计

1. 经常合作的演员有哪些

   为合作创建关联：

   ```cypher
   CALL apoc.periodic.iterate(
   "MATCH (a1:Actors)-[:ACT]->(m:Movies)<-[:ACT]-(a2:Actors)
   RETURN a1, a2, COUNT(m) AS collaborations",
   "MERGE (a1)-[r:COOPERATED]->(a2)
   ON CREATE SET r.count = collaborations
   ON MATCH SET r.count = r.count + collaborations",
   {batchSize: 50, retries: 20, parallel: true}
   ); 
   ```

   ```cypher
   MATCH (a1:Actors)-[r:COOPERATED]-(a2:Actors)
   WHERE r.count > 20
   RETURN a1.Actor_Name, a2.Actor_Name, r.count
   ORDER BY r.count DESC;
   ```

2. 经常合作的导演和演员有哪些

   为合作创建关联：

   ```cypher
   CALL apoc.periodic.iterate(
   "MATCH (a:Actors)-[:ACT]->(m:Movies)<-[:DIRECT]-(d:Directors)
   RETURN a, d, COUNT(m) AS collaborations",
   "MERGE (a)-[r:COOPERATED_WITH_DIRECTOR]->(d)
   ON CREATE SET r.count = collaborations
   ON MATCH SET r.count = r.count + collaborations",
   {batchSize: 50, retries: 20, parallel: true}
   );
   ```

   ```cypher
   MATCH (a:Actors)-[r:COOPERATED_WITH_DIRECTOR]->(d:Directors)
   WHERE r.count > 20
   RETURN a.Actor_Name AS Actor, d.Director_Name AS Director, r.count AS Collaborations
   ORDER BY r.count DESC;
   ```

4. 如果要拍一部XXX类型的电影，最受关注（评论最多）的演员组合（2人）是什么

   ```cypher
   MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres)
   WHERE g.Genre = 'Action'
   WITH m
   MATCH (a1:Actors)-[:ACT]->(m)<-[:ACT]-(a2:Actors)
   WHERE a1.Actor_ID < a2.Actor_ID  // 保证每对演员只匹配一次
   MATCH (m)-[:HAS_REVIEW]->(r:Reviews)
   WITH a1.Actor_Name AS Actor1, a2.Actor_Name AS Actor2, count(r) AS review_count
   WITH Actor1, Actor2, review_count, 
        MAX(review_count) AS max_review_count  // 计算最大评论数
   WHERE review_count = max_review_count  // 只返回评论数为最大值的演员组合
   RETURN Actor1, Actor2, review_count
   ```

   修改图数据库大小，并将评论数直接作为Movies节点的一个属性后

   ```cypher
   MATCH (m:Movies)-[:HAS_GENRE]->(g:Genres)
   WHERE g.Genre = 'Action'
   WITH m
   MATCH (a1:Actors)-[:ACT]->(m)<-[:ACT]-(a2:Actors)
   WHERE a1.Actor_ID < a2.Actor_ID
   WITH a1, a2, sum(m.Reviews_Count) AS total_reviews
   RETURN a1.Actor_Name AS Actor1, a2.Actor_Name AS Actor2, total_reviews
   ORDER BY total_reviews DESC
   LIMIT 1
   ```
   
5. 如果要拍一部XXX类型的电影，最受关注（评论最多）的演员组合（3人）是什么

   ```cypher
   MATCH (g:Genres {Genre: "Action"})<-[:HAS_GENRE]-(m:Movies)
   SET m:FilteredMovie
   RETURN count(m);
   
   MATCH (m:FilteredMovie)<-[:ACT]-(a:Actors)
   WITH m, collect(a.Actor_Name) AS actorNames, m.Reviews_Count AS reviews
   WHERE size(actorNames) >= 3 // 过滤掉少于三人参演的电影
   UNWIND apoc.coll.combinations(actorNames, 3) AS trio
   RETURN trio, sum(reviews) AS totalReviews
   ORDER BY totalReviews DESC
   LIMIT 1;
   ```
   

### 按照上述条件的组合查询和统计

1. XX演员参演了几部XX类型的电影

   ```cypher
   MATCH (a:Actors {Actor_Name: "Andrew Garfield"})-[:ACT]->(m:Movies)-[:HAS_GENRE]->(g:Genres {Genre: "Action"})
   RETURN a.Actor_Name AS Actor, g.Genre AS Genre, count(m) AS MovieCount;
   ```

2. XX演员在XX年参演了多少电影

   ```cypher
   MATCH (a:Actors {Actor_Name: "Andrew Garfield"})-[:ACT]->(m:Movies {Year: 2012})
   RETURN a.Actor_Name AS Actor, m.Year AS Year, count(m) AS MovieCount;
   ```

   



