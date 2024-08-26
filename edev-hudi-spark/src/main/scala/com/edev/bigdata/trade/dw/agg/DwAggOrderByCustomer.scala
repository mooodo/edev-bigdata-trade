package com.edev.bigdata.trade.dw.agg

import com.edev.bigdata.utils.{PropertyFile, SaveConf, SaveUtils, SparkUtils}

/**
 * The aggregation of order by customer
 * @author fangang
 */
object DwAggOrderByCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_customer")
    spark.udf.register("genericKey",(key0: Double, key1: Double)=>key0.toString+"X"+key1.toString)
    val data = spark.sql("select genericKey(o.date_key,o.customer_key) pk," +
      "o.date_key,o.customer_key,sum(o.amount) amount,count(*) cnt,current_timestamp() ts "+
      "from dw.dw_fact_order o join dw.dw_dim_customer c on o.customer_key=c.customer_key "+
      "group by o.date_key,o.customer_key").repartition(num)
    SaveUtils.saveWithPartition(data, SaveConf.build().
      option("tableName","dw.dw_agg_order_customer").
      option("primaryKeyField","pk").
      option("timestampField","ts").
      option("partitionField","date_key"))
  }
}
