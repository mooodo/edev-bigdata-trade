package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The dimension of address
 * @author Fangang
 */
object DwDimAddress {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_address")
    val data = spark.sql("select * from etl.etl_address").repartition(num)
    DataFrameUtils.saveOverwrite(data, "dw", "dw_dim_address")
  }
}