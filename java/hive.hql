-- hive.hql
USE default;

DROP TABLE IF EXISTS visits_by_hospital;
DROP TABLE IF EXISTS hospitals;
DROP TABLE IF EXISTS country_stats;
DROP TABLE IF EXISTS final_stats;

CREATE EXTERNAL TABLE visits_by_hospital (
                                             hospital_id STRING,
                                             year STRING,
                                             total_patients INT,
                                             avg_age DOUBLE
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '${hivevar:input_dir3}';

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
    LOCATION '${hivevar:input_dir4}';

CREATE TABLE country_stats AS
SELECT
    h.country,
    h.type AS hospital_type,
    SUM(v.total_patients) AS total_patients,
    ROUND( (SUM(v.avg_age * v.total_patients) / SUM(v.total_patients)), 2) AS avg_age
FROM visits_by_hospital v
         JOIN hospitals h
              ON v.hospital_id = h.hospital_id
GROUP BY h.country, h.type;

CREATE TABLE final_stats AS
SELECT
    country,
    hospital_type,
    total_patients,
    avg_age,
    RANK() OVER (PARTITION BY country ORDER BY total_patients DESC) AS rank_in_country
FROM country_stats;

INSERT OVERWRITE DIRECTORY '${hivevar:output_dir6}'
    STORED AS TEXTFILE
SELECT CONCAT(
               '{"country":"', country,
               '","hospital_type":"', hospital_type,
               '","total_patients":', cast(total_patients as string),
               ',"avg_age":', cast(avg_age as string),
               ',"rank_in_country":', cast(rank_in_country as string),
               '}'
       ) AS json_line
FROM final_stats;
