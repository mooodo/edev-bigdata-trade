package com.edev.bigdata.trade.dm.broad

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DmBroadOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dm_broad_order")
    val data = spark.sql(
      "select o.order_key,o.date_key,o.customer_key," +
        "o.address_key,o.region_key," +
        "o.amount,o.order_time,o.flag,"+
        "cu.name customer_name,cu.gender,cu.birthdate," +
        "cu.identification,cu.phone_number "+
        "from dw.dw_fact_order o "+
        "join dw.dw_dim_customer cu " +
        "on o.customer_key=cu.customer_key ")
    DataFrameUtils.saveOverwrite(data, "dm", "dm_broad_order")
  }
}
