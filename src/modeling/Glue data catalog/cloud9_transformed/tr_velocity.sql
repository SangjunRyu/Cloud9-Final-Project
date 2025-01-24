CREATE EXTERNAL TABLE `tr_velocity`(
  `link_id` bigint, 
  `lat` double, 
  `lon` double, 
  `district` string, 
  `subdistrict` string, 
  `prcs_spd` string, 
  `prcs_trv_time` string)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string, 
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-transformed/traffic_velocity_transformed'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1737091999')