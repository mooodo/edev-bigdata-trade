package com.edev.bigdata.trade.etl.edevCustomer

import org.apache.spark.sql._
import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about region
 * @author Fangang
 */
object Region {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_region")
    val sc = spark.sparkContext
    
    // save all of the zones
    val zones = spark.sql("select z.id region_key, z.name, p.country_id country_key, c1.name country, "+
      "z.province_id province_key, p.name province, z.city_id city_key, c.name city, "+
      "z.id zone_key, z.name zone "+
      "from edev_customer.t_zone z left join edev_customer.t_province p on z.province_id=p.id "+
      "left join edev_customer.t_city c on z.city_id=c.id left join edev_customer.t_country c1 on p.country_id=c1.id")
    DataFrameUtils.saveOverwrite(zones, "etl", "etl_region")
    
    // save all of the cities
    val cities = spark.sql("select c.id region_key, c.name, p.country_id country_key, c1.name country, "+
      "c.province_id province_key, p.name province, c.id city_key, c.name city, NULL zone_key, NULL zone "+
      "from edev_customer.t_city c left join edev_customer.t_province p on c.province_id=p.id "+
      "left join edev_customer.t_country c1 on p.country_id=c1.id")
    DataFrameUtils.saveAppend(cities, "etl", "etl_region")
    
    // save all of the provinces
    val provinces = spark.sql("select p.id region_key, p.name, p.country_id country_key, c.name country, "+
      "p.id province_key, p.name province, NULL city_key, NULL city, NULL zone_key, NULL zone "+
      "from edev_customer.t_province p left join edev_customer.t_country c on p.country_id=c.id").repartition(num)
    DataFrameUtils.saveAppend(provinces, "etl", "etl_region")
    
    // save the default row
    val defaultList = List(Row(0,"未知地区",0,"未知国家",0,"未知省份",0,"未知城市",0,"未知地区"))
    val schema = zones.schema
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "etl", "etl_region")
  }
}