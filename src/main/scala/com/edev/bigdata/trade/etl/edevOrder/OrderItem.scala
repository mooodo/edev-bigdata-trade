package com.edev.bigdata.trade.etl.edevOrder

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}
/**
 * The ETL process about order item
 * @author Fangang
 */
object OrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_order_item")
    spark.udf.register("nvl", (value:Int) => if(value!=null) value else 0)
    val data = spark.sql("select id order_item_key, order_id order_key, nvl(product_id) product_key, "+
        "quantity, price, amount from edev_order.t_order_item").repartition(num)
    DataFrameUtils.saveOverwrite(data, "etl", "etl_order_item")
  }
}