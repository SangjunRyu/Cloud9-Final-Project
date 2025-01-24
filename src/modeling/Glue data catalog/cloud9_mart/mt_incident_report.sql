CREATE EXTERNAL TABLE `mt_incident_report`(
  `incident_report_id` string, 
  `cause` string, 
  `result` string, 
  `district_name` string, 
  `dong_name` string, 
  `city_rural_category` string, 
  `latitude` double, 
  `longitude` double, 
  `place_name` string, 
  `place_detail` string, 
  `cause_subcategory` string, 
  `fire_station_name` string, 
  `fire_station_center_name` string)
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
  's3://cloud9-mart/incident_report_mart'
TBLPROPERTIES (
  'classification'='parquet', 
  'isRegisteredWithLakeFormation'='false', 
  'spark.sql.create.version'='3.5.2-amzn-1', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.schema'='{\"type\":\"struct\",\"fields\":[{\"name\":\"incident_report_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cause\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"result\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"district_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dong_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city_rural_category\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"latitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longitude\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"place_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"place_detail\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cause_subcategory\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fire_station_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fire_station_center_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_yr\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_mnth\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"report_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='3', 
  'spark.sql.sources.schema.partCol.0'='report_yr', 
  'spark.sql.sources.schema.partCol.1'='report_mnth', 
  'spark.sql.sources.schema.partCol.2'='report_day')