CREATE EXTERNAL TABLE `tr_accident`(
  `acc_id` string COMMENT 'from deserializer', 
  `occr_time` string COMMENT 'from deserializer', 
  `occurred_at` timestamp COMMENT 'from deserializer', 
  `exp_clr_date` string COMMENT 'from deserializer', 
  `exp_clr_time` string COMMENT 'from deserializer', 
  `cleared_at` timestamp COMMENT 'from deserializer', 
  `acc_type` string COMMENT 'from deserializer', 
  `acc_dtype` string COMMENT 'from deserializer', 
  `link_id` string COMMENT 'from deserializer', 
  `latitude` double COMMENT 'from deserializer', 
  `longitude` double COMMENT 'from deserializer', 
  `acc_info` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `occr_date` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION
  's3://cloud9-transformed/accident_transformed'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1736837420')