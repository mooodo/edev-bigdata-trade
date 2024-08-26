package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}

/**
 * The dimension of customer
 * @author Fangang
 */
object DwDimCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_customer")
    val data = spark.sql("select * from etl.etl_customer").repartition(num)
    HudiUtils.saveAppend(data, "dw", "dw_dim_customer",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "customer_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}