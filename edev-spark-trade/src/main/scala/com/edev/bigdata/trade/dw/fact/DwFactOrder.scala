package com.edev.bigdata.trade.dw.fact

import org.apache.spark.sql.SparkSession
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils, UpdateParam, UpdateUtils}

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
    DataFrameUtils.saveOverwrite(data, "dw", "dw_fact_order")

    saveToHistory(spark, num)
  }

  def saveToHistory(spark: SparkSession, numPartitions: Int = 120): Unit = {
    val param = new UpdateParam()
    param.schema = "dw"
    param.table = "dw_fact_order"
    param.partitionField = "datekey"
    param.partitionType = "int"
    param.partedField = "date_key"
    param.keyField = "order_key"
    param.numPartitions = numPartitions
    UpdateUtils.updatePartitionTable(spark, param)
  }
}