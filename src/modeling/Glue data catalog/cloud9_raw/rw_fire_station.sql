CREATE EXTERNAL TABLE `rw_fire_station`(
  `fire_station_name` string, 
  `fire_station_center_name` string, 
  `address` string, 
  `phone` string, 
  `fax` string, 
  `x-axis` double, 
  `y-axis` double, 
  `employee_count` bigint)
PARTITIONED BY ( 
  `year` bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cloud9-batch-raw/fire_station_raw_partitioned'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crw_fire_station', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='76', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'isRegisteredWithLakeFormation'='false', 
  'objectCount'='8', 
  'recordCount'='589', 
  'sizeKey'='45465', 
  'skip.header.line.count'='1', 
  'spark.sql.partitionProvider'='catalog', 
  'typeOfData'='file')