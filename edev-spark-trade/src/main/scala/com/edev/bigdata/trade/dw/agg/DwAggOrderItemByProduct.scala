package com.edev.bigdata.trade.dw.agg

import org.apache.spark.sql.SparkSession
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils, UpdateParam, UpdateUtils}

/**
 * The aggregation of order item by product
 * @author fangang
 */
object DwAggOrderItemByProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_item_product")

    val data = spark.sql("select o.date_key+'X'+o.product_key pk,"+
      "o.date_key,o.product_key,sum(o.amount) amount,count(*) cnt "+
      "from dw.dw_fact_order_item o join dw.dw_dim_product p on o.product_key=p.product_key "+
      "group by o.date_key,o.product_key").repartition(num)
    DataFrameUtils.saveOverwrite(data, "dw", "dw_agg_order_item_product")

    saveToHistory(spark, num)
  }

  def saveToHistory(spark: SparkSession, numPartitions: Int = 120): Unit = {
    val param = new UpdateParam()
    param.schema = "dw"
    param.table = "dw_agg_order_item_product"
    param.partitionField = "datekey"
    param.partitionType = "int"
    param.partedField = "date_key"
    param.keyField = "pk"
    param.numPartitions = numPartitions
    UpdateUtils.updatePartitionTable(spark, param)
  }
}
