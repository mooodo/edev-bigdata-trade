package com.edev.bigdata.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkUtils {
  val hiveDir = PropertyFile.getProperty("hiveDir")
  def init(appName: String) = {
    SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", hiveDir)
      .enableHiveSupport()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }
  
  def getSparkContext(appName: String) = {
    val conf = (new SparkConf).setMaster("local").setAppName(appName);
    new SparkContext(conf)
  }
}