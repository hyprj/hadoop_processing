-- hive.hql
-- Skrypt Hive działający na wynikach MapReduce (input_dir3) i danych o szpitalach (input_dir4)
-- Parametry:
--   ${hiveconf:input_dir3}  - katalog HDFS z wynikami MapReduce
--   ${hiveconf:input_dir4}  - katalog HDFS z danymi o szpitalach
--   ${hiveconf:output_dir6} - katalog HDFS dla wyniku końcowego (JSON)

USE default;

-- Usuwamy stare tabele, jeśli istnieją
DROP TABLE IF EXISTS visits_by_hospital;
DROP TABLE IF EXISTS hospitals;
DROP TABLE IF EXISTS country_stats;
DROP TABLE IF EXISTS final_stats;

-- 1️⃣ Tabela z wyniku MapReduce
CREATE EXTERNAL TABLE visits_by_hospital (
  hospital_id STRING,
  year STRING,
  total_patients INT,
  avg_age DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir3}';

-- 2️⃣ Tabela z danymi o szpitalach
CREATE EXTERNAL TABLE hospitals (
  hospital_id STRING,
  name STRING,
  city STRING,
  country STRING,
  type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir4}';

-- 3️⃣ Tworzymy agregację: kraj + typ szpitala
CREATE TABLE country_stats AS
SELECT
  h.country,
  h.type AS hospital_type,
  SUM(v.total_patients) AS total_patients,
  ROUND(AVG(v.avg_age), 2) AS avg_age
FROM visits_by_hospital v
JOIN hospitals h
  ON v.hospital_id = h.hospital_id
GROUP BY h.country, h.type;

-- 4️⃣ Ranking typów szpitali w ramach kraju
CREATE TABLE final_stats AS
SELECT
  country,
  hospital_type,
  total_patients,
  avg_age,
  RANK() OVER (PARTITION BY country ORDER BY total_patients DESC) AS rank_in_country
FROM country_stats;

-- 5️⃣ Zapis do katalogu wynikowego w formacie JSON
INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir6}'
STORED AS JSONFILE
SELECT * FROM final_stats;
