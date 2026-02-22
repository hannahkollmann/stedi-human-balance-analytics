CREATE EXTERNAL TABLE `stedi.landing_step_trainer`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='distanceFromObject,sensorReadingTime,serialNumber') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-hk-lakehouse/landing/step_trainer/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='66b0bda8-d976-4243-8913-63b3870cc253', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='step_trainer_landing_crawler', 
  'averageRecordSize'='1032', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='3', 
  'recordCount'='3194', 
  'sizeKey'='3298200', 
  'typeOfData'='file')
  