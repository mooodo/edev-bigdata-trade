package com.edev.bigdata.trade.dm.es

import com.edev.bigdata.utils.{EsSparkUtils, PropertyFile}
import org.elasticsearch.spark.sql.EsSparkSQL

object DmEsOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = EsSparkUtils.init("dmEsOrder")

    val result = spark.sql("select * from dm.dm_broad_order").repartition(num)
    EsSparkSQL.saveToEs(result, "dm_es_order")
  }
}
