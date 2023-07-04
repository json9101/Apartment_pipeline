DROP TABLE IF EXISTS base_seoul_apart;

CREATE TABLE IF NOT EXISTS base_seoul_apart  (
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
STORED AS PARQUET
;

INSERT INTO base_seoul_apart (
    Transaction_year,
    District_code,
    Borough_code,
    District_name,
    Borough_name,
    Coordinates,
    Num_small_size,
    Small_size_avg,
    Num_med_small,
    Med_small_avg,
    Num_med,
    Med_avg,
    Num_med_large,
    Med_large_avg,
    Num_larg,
    Larg_avg)
SELECT
    Transaction_year,
    District_code,
    Borough_code,
    District_name,
    Borough_name,
    Coordinates,
    Num_small_size,
    Small_size_avg,
    Num_med_small,
    Med_small_avg,
    Num_med,
    Med_avg,
    Num_med_large,
    Med_large_avg,
    Num_larg,
    Larg_avg
    FROM raw_seoul_apart
;
                      
