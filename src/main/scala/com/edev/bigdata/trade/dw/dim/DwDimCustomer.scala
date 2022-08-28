package com.edev.bigdata.trade.dw.dim

import org.apache.spark.sql.SparkSession
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils, UpdateParam, UpdateUtils}

/**
 * The dimension of customer
 * @author Fangang
 */
object DwDimCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_customer")
    val data = spark.sql("select * from etl.etl_customer").repartition(num)
    DataFrameUtils.saveOverwrite(data, "dw", "dw_dim_customer")
    saveToHistory(spark, num)
  }

  def saveToHistory(spark: SparkSession, numPartitions: Int = 120): Unit = {
    val param = new UpdateParam()
    param.schema = "dw"
    param.table = "dw_dim_customer"
    param.keyField = "customer_key"
    param.numPartitions = numPartitions
    UpdateUtils.updateTable(spark, param)
  }
}