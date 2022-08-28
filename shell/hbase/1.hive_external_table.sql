CREATE EXTERNAL TABLE dm.dm_hbase_customer (
    rowkey string,
    customer map<STRING,STRING>,
    vip map<STRING,STRING>,
    account map<STRING,STRING>,
    update_time string
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,customer:,vip:,account:,update_time:ut")
TBLPROPERTIES ("hbase.table.name" = "dm_hbase_customer");