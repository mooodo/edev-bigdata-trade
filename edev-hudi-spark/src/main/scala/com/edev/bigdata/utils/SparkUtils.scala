package com.edev.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  val hiveDir: String = PropertyFile.getProperty("hiveDir")
  def init(appName: String): SparkSession = {
    SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", hiveDir)
      .enableHiveSupport()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }
  
  def getSparkContext(appName: String): SparkContext = {
    val conf = (new SparkConf).setMaster("local").setAppName(appName);
    new SparkContext(conf)
  }
}