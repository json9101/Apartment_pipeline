DROP TABLE seoul_apartment;

CREATE EXTERNAL TABLE IF NOT EXISTS seoul_apartment (
    Transaction_year INT,
    District_code INT,
    Borough_code INT,
    District_name STRING,
    Borough_name STRING,
    Coordinates STRING,
    Num_small_size INT,
    Small_size_avg DECIMAL(14,2),
    Num_med_small INT,
    Med_small_avg DECIMAL(14,2),
    Num_med  INT,
    Med_avg DECIMAL(14,2),
    Num_med_large INT,
    Med_large_avg DECIMAL(14,2),
    Num_larg INT,
    Larg_avg DECIMAL(14,2))
    -- ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar"     = "\""
    )
    STORED AS TEXTFILE
    LOCATION '/pd24/hive/apart_price_edited/'
    tblproperties ("skip.header.line.count"="1" )
;
