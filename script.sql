-- PASOS:
-- Crear el directorio HDFS: hdfs dfs -mkdir /user/hive/warehouse/employees
-- Importamos los archivos necesarios (txt, log, cvs): hdfs dfs -put [source] /user/hive/warehouse/employees
-- Verificamos: hdfs dfs -ls /user/hive/warehouse/employees
-- hive
-- Creacion de tabla (input)
-- Creacion de tabla resultados (output)
-- https://phoenixnap.com/kb/hive-create-external-table

-- Creacion de la Tabla (input)
CREATE EXTERNAL TABLE IF NOT EXISTS wordcount (
    line STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/employees/';

--------------------------------------------
-- Creacion de la tabla resultados
CREATE TABLE IF NOT EXISTS wordcount_results AS
SELECT word, COUNT(*) as count
FROM (
    SELECT explode(split(line, ' ')) as word
    FROM wordcount
) tmp
GROUP BY word;


-- #####################################

-- Creacion de la tabla logs
CREATE EXTERNAL TABLE IF NOT EXISTS logsUser (
    user_a STRING,
    time_a STRING,
    query_a STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/logUser/';

-------------------------------------------
-- Creacion de la tabla de resultados
CREATE TABLE IF NOT EXISTS logsUser_result AS
SELECT user_a, COUNT(1) AS log_entries
FROM logsUser
GROUP BY user_a
ORDER BY user_a;

-- #####################################

-- Creacion de la tabla visitUser
CREATE EXTERNAL TABLE IF NOT EXISTS visitUser (
    name STRING,
    url STRING,
    time_a STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/visitsUser/';

-------------------------------------------
-- Creacion de la tabla de resultados
CREATE TABLE IF NOT EXISTS visitUser_result AS 
SELECT AVG(num_pages) AS avg_visits
FROM (
    SELECT name, COUNT(1) AS num_pages
    FROM visitUser
    GROUP BY name
) np;

-- #########################################

-- Creacion de las tablas visits y pages:
CREATE EXTERNAL TABLE IF NOT EXISTS rankVisits (
    name STRING,
    url STRING,
    time_a STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/userRank1';

CREATE EXTERNAL TABLE IF NOT EXISTS rankPages (
    url STRING,
    pagerank FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/userRank2';

-- Crear la tabla de resultados:
CREATE TABLE IF NOT EXISTS rank_results AS
SELECT pr.name 
FROM (
    SELECT V.name, AVG(P.pagerank) AS prank
    FROM rankVisits V 
    JOIN rankPages P ON (V.url = P.url)
    GROUP BY V.name
) pr
WHERE pr.prank > 0.5;

