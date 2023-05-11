package com.edev.bigdata.trade.etl.edevCustomer

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about address
 *
 * @author Fangang
 */
object Address {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_address")
    spark.udf.register("getRegionKey", (countryId:Int, provinceId:Int, cityId:Int, zoneId:Int) =>
      if(zoneId!=null) zoneId else if(cityId!=null) cityId else if(provinceId!=null) provinceId else 0)
    val data = spark.sql("select id address_key, customer_id customer_key, getRegionKey(country_id, "+
        "province_id, city_id, zone_id) region_key, address, phone_number from edev_customer.t_address").repartition(num)
    DataFrameUtils.saveOverwrite(data, "etl", "etl_address")
  }
}