CREATE EXTERNAL TABLE `rw_district_seoul`(
  `district_name` string, 
  `dong_name` string, 
  `population` bigint, 
  `area` double, 
  `density` bigint)
PARTITIONED BY ( 
  `year` bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cloud9-batch-raw/district_seoul_raw_partitioned'
TBLPROPERTIES (
  'classification'='csv', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"district_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dong_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"population\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"area\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"density\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='year')