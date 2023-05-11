package com.edev.bigdata.trade.etl.edevOrder

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about order
 * @author Fangang
 */
object Order {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_order")
    spark.udf.register("nvl", (value:Int) => if(Option(value).isDefined) value else 0)
    val data = spark.sql("select id order_key, nvl(customer_id) customer_key, nvl(address_id) address_key, "+
        "amount, order_time, flag from edev_order.t_order").repartition(num)
    DataFrameUtils.saveOverwrite(data, "etl", "etl_order")
  }
}