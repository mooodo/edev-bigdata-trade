package com.edev.bigdata.utils

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{Dataset, Row}

object HudiUtils {

  def saveOverwrite(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      options(conf.getAllConfigs).
      mode(Overwrite).
      saveAsTable(database+"."+table)
  }

  def saveAppend(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      options(conf.getAllConfigs).
      mode(Append).
      saveAsTable(database+"."+table)
  }

  def delete(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION.key(), "delete").
      options(conf.getAllConfigs).
      mode(Append).
      saveAsTable(database+"."+table)
  }

  def saveWithPartition(data: Dataset[Row], params: SaveParams): Unit = {
    data.createOrReplaceTempView("tmp")
    data.sparkSession.sql(s"create table if not exists ${params.tableName} using hudi "+
      s"tblproperties(type='cow',primaryKey='${params.primaryKeyField}',preCombineField='${params.timestampField}')"+
      s"partitioned by (${params.partitionField}) as "+
      "select * from tmp where 0=1;")
    data.sparkSession.sql(s"insert into ${params.tableName} select * from tmp")
  }

  class SaveParams {
    var tableName: String = _
    var primaryKeyField: String = _
    var timestampField: String =_
    var partitionField: String =_
  }
}
