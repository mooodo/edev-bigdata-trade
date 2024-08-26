package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{DateUtils, HudiConf, HudiUtils, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
    HudiUtils.saveAppend(data, "dw", "dw_dim_date",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "date_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}