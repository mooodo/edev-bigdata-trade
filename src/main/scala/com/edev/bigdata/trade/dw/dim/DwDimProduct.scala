package com.edev.bigdata.trade.dw.dim

import org.apache.spark.sql.SparkSession
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils, UpdateParam, UpdateUtils}

/**
 * The dimension of product
 * @author Fangang
 */
object DwDimProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_product")
    val result = spark.sql("select * from etl.etl_product")
      .repartition(num)
    DataFrameUtils.saveOverwrite(result, "dw", "dw_dim_product")
    saveToHistory(spark, num)
  }

  def saveToHistory(spark: SparkSession, numPartitions: Int = 120): Unit = {
    val param = new UpdateParam()
    param.schema = "dw"
    param.table = "dw_dim_product"
    param.keyField = "product_key"
    param.numPartitions = numPartitions
    UpdateUtils.updateTable(spark, param)
  }
}