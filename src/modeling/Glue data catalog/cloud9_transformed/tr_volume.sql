CREATE EXTERNAL TABLE `tr_volume`(
  `spot_num` string, 
  `io_type` string, 
  `lane_num` string, 
  `vol` string)
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
  's3://cloud9-transformed/traffic_volume_transformed'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1737092060')