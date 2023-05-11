package com.edev.bigdata.trade.dw.dim

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.edev.bigdata.utils.{DataFrameUtils, DateUtils, SparkUtils}

/**
 * The dimension of date
 * @author Fangang
 */
object DwDimDate{
  def main(args: Array[String]){
    val spark = SparkUtils.init("dw_dim_date")
    val sc = spark.sparkContext
    val strStart = args.apply(0)
    val dateStart = DateUtils.getTime(strStart, "yyyy-MM")
    val strEnd = args.apply(1)
    val dateEnd = DateUtils.getTime(strEnd, "yyyy-MM")
    val list = List()
    val listDate = DateUtils.getMonthsBetween(dateStart,dateEnd,list)
    
    val rows = listDate.map { 
      x => Row(
          DateUtils.format(x, "yyyyMM").toInt, 
          DateUtils.format(x, "yyyy"),
          DateUtils.format(x, "MM"),
          DateUtils.format(x, "yyyyMM").toInt,
          DateUtils.format(x, "yyyy年"),
          DateUtils.format(x, "MM月"),
          DateUtils.format(x, "yyyy年MM月")
        )}
   
     val schema = StructType(Array(
      StructField("date_key", IntegerType, false),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("yyyymm", IntegerType, true),
      StructField("year_desc", StringType, true),
      StructField("month_desc", StringType, true),
      StructField("yyyymm_desc", StringType, true)
     ))
    val result = sc.parallelize(rows)
    val data = spark.createDataFrame(result, schema)
    DataFrameUtils.saveOverwrite(data, "dw", "dw_dim_date")
  }
}