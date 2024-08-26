package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}

/**
 * The dimension of address
 * @author Fangang
 */
object DwDimAddress {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_address")
    val data = spark.sql("select * from etl.etl_address").repartition(num)
    HudiUtils.saveOverwrite(data, "dw", "dw_dim_address",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "address_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}