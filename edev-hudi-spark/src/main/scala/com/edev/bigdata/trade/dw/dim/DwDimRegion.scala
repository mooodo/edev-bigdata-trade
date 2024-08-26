package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

/**
 * The dimension of region
 * @author Fangang
 */
object DwDimRegion {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_region")
    val data = spark.sql("select * from etl.etl_region").repartition(num)
    HudiUtils.saveOverwrite(data, "dw", "dw_dim_region",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "region_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}