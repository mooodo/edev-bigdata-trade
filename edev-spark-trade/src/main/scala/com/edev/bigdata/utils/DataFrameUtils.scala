package com.edev.bigdata.utils

import org.apache.spark.sql.{Dataset, Row, SaveMode}

/**
 * The Data Frame utilities
 * @author Fangang
 */
object DataFrameUtils {
  /**
   * save data into the table append.
   * @param data the data which need save
   * @param database the hive database which need save to
   * @param table the hive table which need save to
   */
  def saveAppend(data: Dataset[Row], database: String, table: String): Unit = {
    data.write.mode(SaveMode.Append).saveAsTable(database+"."+table)
  }
  /**
   * save data into the table overwrite.
   * @param data the data which need save
   * @param database the hive database which need save to
   * @param table the hive table which need save to
   */
  def saveOverwrite(data: Dataset[Row], database: String, table: String): Unit = {
    data.write.mode(SaveMode.Overwrite).saveAsTable(database+"."+table)
  }
}