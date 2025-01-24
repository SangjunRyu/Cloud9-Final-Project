CREATE EXTERNAL TABLE `tr_district_seoul`(
  `district_name` string, 
  `dong_name` string, 
  `population` bigint, 
  `area` double, 
  `density` bigint)
PARTITIONED BY ( 
  `year` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-transformed/district_seoul_transformed'
TBLPROPERTIES (
  'classification'='parquet', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"district_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dong_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"population\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"area\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"density\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='year')