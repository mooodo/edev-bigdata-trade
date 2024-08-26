package com.edev.bigdata.trade.dw.fact

import com.edev.bigdata.utils.{PropertyFile, SaveConf, SaveUtils, SparkUtils}

/**
 * The fact of order item
 * @author Fangang
 */
object DwFactOrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_fact_order_item")
    spark.udf.register("getDateKey", (date: String) => (date.substring(0, 4)+date.substring(5, 7)).toInt)
    val data = spark.sql("select order_item_key, getDateKey(o.order_time) date_key, oi.order_key, "+
        "o.customer_key, o.address_key, a.region_key, oi.product_key, p.classify_key, p.supplier_key, "+
        "o.order_time, quantity, oi.price, oi.amount "+
        "from etl.etl_order_item oi left join dw.dw_dim_product p on oi.product_key=p.product_key "+
        "left join dw.dw_fact_order o on oi.order_key=o.order_key "+
        "left join dw.dw_dim_address a on o.address_key=a.address_key").repartition(num)
    //DataFrameUtils.saveOverwrite(data, "dw", "dw_fact_order_item")
    SaveUtils.saveWithPartition(data, SaveConf.build().
      option("tableName","dw.dw_fact_order_item").
      option("primaryKeyField","order_key").
      option("timestampField","order_time").
      option("partitionField","date_key"))
  }
}