CREATE EXTERNAL TABLE `mt_district_seoul_grouped`(
  `district_name` string, 
  `sum(population)` int, 
  `sum(area)` double)
PARTITIONED BY ( 
  `year` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-mart/district_seoul_grouped_mart'
TBLPROPERTIES (
  'classification'='parquet', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"district_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"sum(population)\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"sum(area)\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='year')