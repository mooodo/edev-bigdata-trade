package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

/**
 * The dimension of product
 * @author Fangang
 */
object DwDimProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_product")
    val result = spark.sql("select * from etl.etl_product")
      .repartition(num)
    HudiUtils.saveAppend(result, "dw", "dw_dim_product",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "product_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}