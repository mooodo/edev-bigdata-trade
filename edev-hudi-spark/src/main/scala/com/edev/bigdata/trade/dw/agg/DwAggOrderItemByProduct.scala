package com.edev.bigdata.trade.dw.agg

import com.edev.bigdata.utils.{PropertyFile, SaveConf, SaveUtils, SparkUtils}

/**
 * The aggregation of order item by product
 * @author fangang
 */
object DwAggOrderItemByProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_item_product")
    spark.udf.register("genericKey",(key0: Double, key1: Double)=>key0.toString+"X"+key1.toString)
    val data = spark.sql("select genericKey(o.date_key,o.product_key) pk,"+
      "o.date_key,o.product_key,sum(o.amount) amount,count(*) cnt,current_timestamp() ts "+
      "from dw.dw_fact_order_item o join dw.dw_dim_product p on o.product_key=p.product_key "+
      "group by o.date_key,o.product_key").repartition(num)
    SaveUtils.saveWithPartition(data, SaveConf.build().
      option("tableName","dw.dw_agg_order_item_product").
      option("primaryKeyField","pk").
      option("timestampField","ts").
      option("partitionField","date_key"))
  }
}
