package com.edev.bigdata.trade.dm.broad

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DmBroadOrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dm_broad_order_item")
    val data = spark.sql("select oi.*,"+
      "cu.name customer_name,cu.gender,cu.birthdate,cu.identification,cu.phone_number,"+
      "p.name product_name,p.unit,p.image,p.original_price,p.tip "+
      "from dw.dw_fact_order_item oi "+
      "join dw.dw_dim_customer cu on oi.customer_key=cu.customer_key "+
      "join dw.dw_dim_product p on oi.product_key=p.product_key ")
    DataFrameUtils.saveOverwrite(data, "dm", "dm_broad_order_item")
  }
}
