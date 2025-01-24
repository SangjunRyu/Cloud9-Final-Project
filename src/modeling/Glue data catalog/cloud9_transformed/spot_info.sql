CREATE EXTERNAL TABLE `spot_info`(
  `spot_num` string, 
  `spot_nm` string, 
  `lon` double, 
  `lat` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cloud9-transformed/spot_info'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY', 
  'transient_lastDdlTime'='1736910885')