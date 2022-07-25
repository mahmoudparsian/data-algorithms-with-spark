CREATE EXTERNAL TABLE `continents`(
  `country` string,
  `city` string,
  `temperature` integer
)
PARTITIONED BY (
  `continent` string
)
STORED AS PARQUET
LOCATION 's3://mybucket/SCU/OUTPUT2/continents_countries1/'
tblproperties ("parquet.compress"="SNAPPY");
