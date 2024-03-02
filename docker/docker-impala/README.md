# HDFS 실행파일로 연결
bin/hdfs dfs -fs hdfs://localhost:32955 -ls -R /
 
 
# WebHDFS로 연결
 
## 디렉터리 추가
curl -i -X PUT 'http://localhost:42501/webhdfs/v1/hello?user.name=<사용자이름>&op=MKDIRS&permission=755'

#### Response
HTTP/1.1 307 Temporary Redirect
Date: Fri, 11 Aug 2023 08:44:05 GMT
Cache-Control: no-cache
Expires: Fri, 11 Aug 2023 08:44:05 GMT
Date: Fri, 11 Aug 2023 08:44:05 GMT
Pragma: no-cache
X-FRAME-OPTIONS: SAMEORIGIN
Set-Cookie: hadoop.auth="u=<사용자이름>&p=<사용자이름>&t=simple&e=1691779445033&s=EHYpjzMOFQInpQT9x5qMPH63ea4="; Path=/; HttpOnly
Location: http://127.0.0.1:46437/webhdfs/v1/hello/README.md?op=CREATE&user.name=<사용자이름>&namenoderpcaddress=127.0.0.1:32955&createflag=&createparent=true&overwrite=false
Content-Type: application/octet-stream
Content-Length: 0
 
 
 
## 파일 추가
### 1. 네임노드 파일 생성 기능 호출
curl -i -X PUT 'http://localhost:42501/webhdfs/v1/hello/README.md?user.name=<사용자이름>&op=CREATE'
#### Response
HTTP/1.1 307 Temporary Redirect
Date: Fri, 11 Aug 2023 08:44:05 GMT
Cache-Control: no-cache
Expires: Fri, 11 Aug 2023 08:44:05 GMT
Date: Fri, 11 Aug 2023 08:44:05 GMT
Pragma: no-cache
X-FRAME-OPTIONS: SAMEORIGIN
Set-Cookie: hadoop.auth="u=<사용자이름>&p=<사용자이름>&t=simple&e=1691779445033&s=EHYpjzMOFQInpQT9x5qMPH63ea4="; Path=/; HttpOnly
Location: http://127.0.0.1:46437/webhdfs/v1/hello/README.md?op=CREATE&user.name=<사용자이름>&namenoderpcaddress=127.0.0.1:32955&createflag=&createparent=true&overwrite=false
Content-Type: application/octet-stream
Content-Length: 0
 
### 2. 위 Location 주소(데이터노드)로 파일 추가 확정
curl -i -X PUT -T README.md 'http://127.0.0.1:46437/webhdfs/v1/hello/README.md?op=CREATE&user.name=<사용자이름>&namenoderpcaddress=127.0.0.1:32955&createflag=&createparent=true&overwrite=false'
#### Response
HTTP/1.1 201 Created
Location: hdfs://127.0.0.1:32955/hello/README.md
Content-Length: 0
Access-Control-Allow-Origin: *
Connection: close
 
 
 
## 디렉터리 목록 조회
curl -i -X GET 'http://localhost:42501/webhdfs/v1/hello?user.name=admin&op=LISTSTATUS'
#### Response
{"FileStatuses":{"FileStatus":[
{"accessTime":1691743486609,"blockSize":134217728,"childrenNum":0,"fileId":16387,"group":"supergroup","length":5597,"modificationTime":1691743487104,"owner":"<사용자이름>","pathSuffix":"README.md","permission":"644","replication":1,"storagePolicy":0,"type":"FILE"}
]}}
 
 
 
## 파일 읽기
curl -i -L -X GET 'http://localhost:42501/webhdfs/v1/hello/README.md?user.name=admin&op=OPEN'



-- INSERT INTO actors VALUES
--   (1, 'Tom', 'Cruise', 59)
-- , (2, 'Brad', 'Pitt', 58)
-- , (3, 'Leonardo', 'DiCaprio', 48)
-- , (4, 'Meryl', 'Streep', 73)
-- , (5, 'Denzel', 'Washington', 67)
-- , (6, 'Angelina', 'Jolie', 47)
-- , (7, 'Robert', 'Downey Jr.', 57)
-- , (8, 'Jennifer', 'Lawrence', 32)
-- , (9, 'Johnny', 'Depp', 60)
-- , (10, 'Charlize', 'Theron', 47)
-- ;

docker run --rm -it \
--network spark-test \
-v .:/opt/impala/examples \
-e IMPALAD=impalad:21050 \
--entrypoint /opt/impala/examples/client/entrypoint.sh \
apache/impala:81d5377c2-impala_quickstart_client \
impala-shell -f /opt/impala/examples/init.sql;

docker run --rm -it \
--network spark-test \
-v .:/opt/impala/examples \
-e IMPALAD=impalad:21050 \
--entrypoint /opt/impala/examples/client/entrypoint.sh \
apache/impala:81d5377c2-impala_quickstart_client \
impala-shell -q 'select * from films_kudu';






docker run --rm -it \
--network spark-test \
-v .:/opt/impala/examples \
-v ./hive/conf:/opt/impala/conf:ro \
-v ./impala:/opt/impala/custom:ro \
-e IMPALAD=impalad:21050 \
--entrypoint /opt/impala/custom/cli ent/entrypoint.sh \
apache/impala:81d5377c2-impala_quickstart_client \
impala-shell;




docker run --rm -it \
--network spark-test \
-v .:/opt/impala/examples \
-v ./docker-impala/hive/conf:/opt/impala/conf:ro \
-v ./docker-impala/impala:/opt/impala/custom:ro \
-e IMPALAD=impalad:21050 \
--entrypoint /opt/impala/custom/cli ent/entrypoint.sh \
apache/impala:81d5377c2-impala_quickstart_client \
impala-shell;






https://github.com/jpype-project/jpype/

# Apache Logging
apache-log4j-extras-1.2.17.jar
commons-logging-1.2.jar
slf4j-api-2.0.7.jar
# Hadoop MiniCluster
hadoop-client-api-3.0.0.jar
hadoop-client-minicluster-3.0.0.jar
hadoop-client-runtime-3.0.0.jar
# MiniCluster 내부에 필요한 jar
htrace-core4-4.2.0-incubating.jar
junit-4.13.2.jar



show create table crm.actors


result
CREATE TABLE crm.actors (text STRING) COMMENT 'Actors' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' WITH SERDEPROPERTIES (
  'field.delim' = ',',
  'line.delim' = '\n',
  'serialization.format' = ','
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/user/hive/warehouse/managed/crm.db/actors' TBLPROPERTIES (
  'OBJCAPABILITIES' = 'HIVEMANAGEDINSERTREAD,HIVEMANAGEDINSERTWRITE',
  'transactional' = 'true',
  'transactional_properties' = 'insert_only'
)

insert into crm.actors select '111'

# hdfs dfs -getfacl hdfs://namenode:8020/user/hive/warehouse/managed/crm.db
# hdfs dfs -setfacl -m user:flink:rwx hdfs://namenode:8020/user/hive/warehouse/managed
# hdfs dfs -setfacl -R -m user:flink:rwx hdfs://namenode:8020/user/hive/warehouse/managed/crm.db