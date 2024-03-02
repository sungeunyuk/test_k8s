CREATE TABLE IF NOT EXISTS actors
(
    id        INT,
    firstname STRING,
    lastname  STRING,
    age       INT
)
    COMMENT 'Actors'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
;

-- LOAD DATA LOCAL INPATH 'resources/data/actors_csv'
-- OVERWRITE INTO TABLE actors
-- INPUTFORMAT 'csv' SERDE 'serde'
-- ;


INSERT INTO actors VALUES
  (1, 'Tom', 'Cruise', 59)
, (2, 'Brad', 'Pitt', 58)
, (3, 'Leonardo', 'DiCaprio', 48)
, (4, 'Meryl', 'Streep', 73)
, (5, 'Denzel', 'Washington', 67)
, (6, 'Angelina', 'Jolie', 47)
, (7, 'Robert', 'Downey Jr.', 57)
, (8, 'Jennifer', 'Lawrence', 32)
, (9, 'Johnny', 'Depp', 60)
, (10, 'Charlize', 'Theron', 47)
;