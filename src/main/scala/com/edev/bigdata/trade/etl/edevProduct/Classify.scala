package com.edev.bigdata.trade.etl.edevProduct

import com.edev.bigdata.utils.{DataFrameUtils, PropertyFile, SparkUtils}

/**
 * The ETL process about classify
 * @author Fangang
 */
object Classify {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_classify")
    
    // save the layer 2 classifies
    val layer2 = spark.sql("select c2.id classify_key, c2.name, c1.parent_id layer0_key, c0.name layer0, "+
        "c2.parent_id layer1_key, c1.name layer1, c2.id layer2_key, c2.name layer2 "+
        "from edev_product.t_classify c2 left join edev_product.t_classify c1 on c2.parent_id=c1.id "+
        "left join edev_product.t_classify c0 on c1.parent_id=c0.id  where c2.layer=2").repartition(num)
    DataFrameUtils.saveOverwrite(layer2, "etl", "etl_classify")
    
    // save the layer 1 classifies
    val layer1 = spark.sql("select c1.id classify_key, c1.name, c1.parent_id layer0_key, c0.name layer0, "+
        "c1.id layer1_key, c1.name layer1, NULL layer2_key, NULL layer2 "+
        "from edev_product.t_classify c1 left join edev_product.t_classify c0 on c1.parent_id=c0.id where c1.layer=1").repartition(num)
    DataFrameUtils.saveAppend(layer1, "etl", "etl_classify")
    
    // save the layer 0 classifies
    val layer0 = spark.sql("select id classify_key, name, id layer0_key, name layer0, "+
        "NULL layer1_key, NULL layer1, NULL layer2_key, NULL layer2 "+
        "from edev_product.t_classify where layer=0").repartition(num)
    DataFrameUtils.saveAppend(layer0, "etl", "etl_classify")
  }
}