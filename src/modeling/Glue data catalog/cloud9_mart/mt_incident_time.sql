CREATE EXTERNAL TABLE `mt_incident_time`(
  `incident_report_id` string, 
  `report_date` date, 
  `report_time` int, 
  `dispatch_date` date, 
  `dispatch_time` int, 
  `arrival_date` date, 
  `arrival_time` int, 
  `completion_date` date, 
  `completion_time` int, 
  `return_date` date, 
  `return_time` int, 
  `report_timestamp` timestamp, 
  `arrival_timestamp` timestamp, 
  `time_difference` double)
PARTITIONED BY ( 
  `report_yr` string, 
  `report_mnth` string, 
  `report_day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-mart/incident_time_mart'
TBLPROPERTIES (
  'classification'='parquet', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"incident_report_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_time\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dispatch_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dispatch_time\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"arrival_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"arrival_time\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"completion_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"completion_time\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"return_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"return_time\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_timestamp\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"arrival_timestamp\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"time_difference\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_yr\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_mnth\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='3', 
  'spark.sql.sources.schema.partCol.0'='report_yr', 
  'spark.sql.sources.schema.partCol.1'='report_mnth', 
  'spark.sql.sources.schema.partCol.2'='report_day')