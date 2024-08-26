package com.edev.bigdata.trade.dw.dim

import com.edev.bigdata.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

/**
 * The dimension of product classify
 * @author Fangang
 */
object DwDimClassify {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_classify")
    val data = spark.sql("select * from etl.etl_classify").repartition(num)
    HudiUtils.saveOverwrite(data, "dw", "dw_dim_classify",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "classify_key")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}