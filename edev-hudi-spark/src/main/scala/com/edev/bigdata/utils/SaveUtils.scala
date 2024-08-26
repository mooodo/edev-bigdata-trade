package com.edev.bigdata.utils

import org.apache.spark.sql.{Dataset, Row}

object SaveUtils {
  def saveWithPartition(data: Dataset[Row], conf: SaveConf): Unit = {
    val tableName = conf.get("tableName")
    val primaryKeyField = conf.get("primaryKeyField")
    val timestampField = conf.get("timestampField")
    val partitionField = conf.get("partitionField")
    data.createOrReplaceTempView("tmp")
    data.sparkSession.sql(s"create table if not exists ${tableName} using hudi "+
      s"tblproperties(type='cow',primaryKey='${primaryKeyField}',preCombineField='${timestampField}')"+
      s"partitioned by (${partitionField}) as select * from tmp where 0=1;")
    data.sparkSession.sql(s"insert into ${tableName} select * from tmp")
  }
}