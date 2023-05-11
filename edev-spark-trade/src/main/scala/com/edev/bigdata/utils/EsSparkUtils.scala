package com.edev.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object EsSparkUtils {
  val hiveDir = PropertyFile.getProperty("hiveDir")
  val esNodes = PropertyFile.getProperty("esNodes")
  def init(appName: String) = {
    SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", hiveDir)
      .config("es.index.auto.create", "true")
      .config("es.nodes", esNodes)
      .enableHiveSupport()
      .appName(appName)
      .getOrCreate()
  }

  def getSparkContext(appName: String) = {
    val conf = (new SparkConf).setMaster("local").setAppName(appName);
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", esNodes)
    new SparkContext(conf)
  }
}
