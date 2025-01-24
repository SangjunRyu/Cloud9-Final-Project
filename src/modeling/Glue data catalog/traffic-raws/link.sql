CREATE EXTERNAL TABLE `traffic_volume`(
  `date` bigint, 
  `day_of_week` string, 
  `location` string, 
  `location_id` string, 
  `direction` string, 
  `category` string, 
  `hour_0` double, 
  `hour_1` double, 
  `hour_2` double, 
  `hour_3` double, 
  `hour_4` double, 
  `hour_5` double, 
  `hour_6` double, 
  `hour_7` double, 
  `hour_8` double, 
  `hour_9` double, 
  `hour_10` double, 
  `hour_11` double, 
  `hour_12` double, 
  `hour_13` double, 
  `hour_14` double, 
  `hour_15` double, 
  `hour_16` double, 
  `hour_17` double, 
  `hour_18` double, 
  `hour_19` double, 
  `hour_20` double, 
  `hour_21` double, 
  `hour_22` double, 
  `hour_23` double)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://ysj-traffic-info/traffic_volume_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1735525508')