package com.edev.bigdata.utils

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * The utilities of the hdfs operations
 *
 * @author Fangang
 */
object HdfsUtils {
  /**
   * get data from hdfs by a table without any partition
   * @param spark
   * @param schema
   * @param table
   * @return data, or empty data frame if the file is not exist.
   */
  def getDataByTable(spark: SparkSession, schema: String, table: String): Dataset[Row] = {
    val targetDir = getTargetDir()
    val path = targetDir + schema + ".db/" + table
    getDataByPath(spark, path)
  }

  /**
   * get data from hdfs by a certain partition of a table
   * @param spark
   * @param schema
   * @param table
   * @param partition
   * @param value
   * @return data, or empty data frame if the file is not exist.
   */
  def getDataByPartition(spark: SparkSession, schema: String, table: String, partition: String, value: String): Dataset[Row] = {
    val targetDir = getTargetDir()
    val path = targetDir + schema + ".db/" + table + "/" + partition + "=" + value
    getDataByPath(spark, path)
  }

  /**
   * get data from hdfs between one more partitions of a table
   * @param spark
   * @param schema
   * @param table
   * @param partition
   * @param begin
   * @param end
   * @return data, or empty data frame if the file is not exist.
   */
  def getDataBetweenPartitions(spark: SparkSession, schema: String, table: String, partition: String, begin: Int, end: Int) = {
    def union(data: Dataset[Row], value: Int):Dataset[Row] = {
      if(value > end) data
      else {
        val df = getDataByPartition(spark, schema, table, partition, value.toString)
        val res = if(df!=null && !df.isEmpty) data.union(df) else data
        union(res, value+1)
      }
    }
    if(begin > end) throw new RuntimeException;
    val data = getDataByPartition(spark, schema, table, partition, begin.toString)
    union(data, begin+1)
  }

  /**
   * get data from hdfs by path
   * @param spark
   * @param path
   * @return data, or empty data frame if the file is not exist.
   */
  def getDataByPath(spark: SparkSession, path: String): Dataset[Row] = {
    try {
      spark.read.parquet(path)
    } catch { case t: Throwable => spark.emptyDataFrame}
  }

  def getTargetDir(): String = {
    var targetDir = PropertyFile.getProperty("targetDir")
    if (targetDir.substring(targetDir.length - 1).equals("/")) targetDir
    else targetDir + "/"
  }
}
