CREATE EXTERNAL TABLE `continents_2`(
  `city` string,
  `temperature` integer
)
PARTITIONED BY (
  `continent` string,
  `country` string
)
STORED AS PARQUET
LOCATION 's3://mybucket/SCU/OUTPUT2/continents_countries2/'
tblproperties ("parquet.compress"="SNAPPY");
