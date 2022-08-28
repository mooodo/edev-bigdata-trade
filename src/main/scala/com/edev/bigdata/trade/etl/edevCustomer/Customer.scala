package com.edev.bigdata.trade.etl.edevCustomer

import org.apache.spark.sql._
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about customer
 * @author Fangang
 */
object Customer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_customer")
    val data = spark.sql("select id customer_key, name, gender, birthdate, "+
        "identification, phone_number from edev_customer.t_customer")
      .repartition(num)
    DataFrameUtils.saveOverwrite(data, "etl", "etl_customer")
    
    // save the default row
    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知客户",null,null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "etl", "etl_customer")
  }
}