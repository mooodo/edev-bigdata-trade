package com.edev.bigdata.trade.dw.fact

import com.edev.bigdata.utils.{PropertyFile, SaveConf, SaveUtils, SparkUtils}

/**
 * The fact of order
 * @author Fangang
 */
object DwFactOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_fact_order")
    spark.udf.register("getDateKey", (date: String) => (date.substring(0, 4)+date.substring(5, 7)).toInt)
    val data = spark.sql("select order_key, getDateKey(order_time) date_key, o.customer_key, o.address_key, "+
        "a.region_key, amount, order_time, flag from etl.etl_order o "+
        "left join dw.dw_dim_address a on o.address_key=a.address_key ").repartition(num)
    //DataFrameUtils.saveOverwrite(data, "dw", "dw_fact_order")
    SaveUtils.saveWithPartition(data, SaveConf.build().
      option("tableName","dw.dw_fact_order").
      option("primaryKeyField","order_key").
      option("timestampField","order_time").
      option("partitionField","date_key"))
  }
}