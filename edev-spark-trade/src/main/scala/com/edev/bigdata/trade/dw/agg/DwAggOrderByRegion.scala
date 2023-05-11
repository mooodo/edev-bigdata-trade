package com.edev.bigdata.trade.dw.agg

import org.apache.spark.sql.SparkSession
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils, UpdateParam, UpdateUtils}

/**
 * The aggregation of order by region
 * @author fangang
 */
object DwAggOrderByRegion {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_region")

    val data = spark.sql("select o.date_key+'X'+o.region_key pk," +
      "o.date_key,o.region_key,sum(o.amount) amount,count(*) cnt "+
      "from dw.dw_fact_order o join dw.dw_dim_region r on o.region_key=r.region_key "+
      "group by o.date_key,o.region_key").repartition(num)
    DataFrameUtils.saveOverwrite(data, "dw", "dw_agg_order_region")

    saveToHistory(spark, num)
  }

  def saveToHistory(spark: SparkSession, numPartitions: Int = 120): Unit = {
    val param = new UpdateParam()
    param.schema = "dw"
    param.table = "dw_agg_order_region"
    param.partitionField = "datekey"
    param.partitionType = "int"
    param.partedField = "date_key"
    param.keyField = "pk"
    param.numPartitions = numPartitions
    UpdateUtils.updatePartitionTable(spark, param)
  }
}
