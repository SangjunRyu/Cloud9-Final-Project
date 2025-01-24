CREATE EXTERNAL TABLE `tr_fire_station`(
  `fire_station_name` string, 
  `fire_station_center_name` string, 
  `address` string, 
  `phone` string, 
  `fax` string, 
  `latitude` double, 
  `longitude` double, 
  `employee_count` bigint)
PARTITIONED BY ( 
  `year` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-transformed/fire_station_transformed'
TBLPROPERTIES (
  'classification'='parquet', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"fire_station_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fire_station_center_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"address\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"phone\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fax\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"latitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"employee_count\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='year')