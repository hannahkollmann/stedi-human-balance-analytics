CREATE EXTERNAL TABLE `stedi.landing_accelerometer`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='timestamp,user,x,y,z') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-hk-lakehouse/landing/accelerometer/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='c225f67d-3701-482d-aee5-cf8dbfe30f04', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='accelerometer_landing_crawler', 
  'averageRecordSize'='761', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='9', 
  'recordCount'='9007', 
  'sizeKey'='6871328', 
  'typeOfData'='file')
