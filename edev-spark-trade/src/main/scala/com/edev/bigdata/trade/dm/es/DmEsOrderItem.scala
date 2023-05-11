package com.edev.bigdata.trade.dm.es

import org.elasticsearch.spark.sql.EsSparkSQL
import com.edev.bigdata.utils.{EsSparkUtils, PropertyFile}

object DmEsOrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = EsSparkUtils.init("dmEsOrderItem")

    val result = spark.sql("select * from dm.dm_broad_order_item").repartition(num)
    EsSparkSQL.saveToEs(result, "dm_es_order_item")
  }

}
