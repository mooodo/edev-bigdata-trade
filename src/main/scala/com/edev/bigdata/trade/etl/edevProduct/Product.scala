package com.edev.bigdata.trade.etl.edevProduct

import org.apache.spark.sql._
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about product
 * @author Fangang
 */
object Product {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_product")
    spark.udf.register("nvl", (value:Int) => if(value!=null) value else 0)
    val data = spark.sql("select id product_key, name, price, unit, nvl(supplier_id) supplier_key, "+
        "nvl(classify_id) classify_key, image, original_price, tip from edev_product.t_product").repartition(num)
    DataFrameUtils.saveOverwrite(data, "etl", "etl_product")
    
    // save the default row
    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知产品",null,null,0,0,null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "etl", "etl_product")
  }
}