package com.edev.bigdata.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object UpdateUtils {
  /**
   * update a table without partition
   * @param spark SparkSession
   * @param param UpdateParam
   */
  def updateTable(spark:SparkSession, param:UpdateParam):Unit = {
    val num = param.numPartitions

    /**
     * get incremental data which collected this time
     * @param spark SparkSession
     * @param param UpdateParam
     * @return the incremental data
     */
    def getIncData(spark:SparkSession, param: UpdateParam):DataFrame = {
      spark.sql(s"select * from ${param.schema}.${param.table} ").repartition(num)
    }

    /**
     * create the historical table if not exists
     * @param spark SparkSession
     * @param param UpdateParam
     */
    def createHitTable(spark:SparkSession, param:UpdateParam): Unit = {
      val sql = new StringBuilder
      sql.append(s"create table if not exists ${param.schema}.${param.table}_hit (")
      sql.append(getDescription(spark, param.schema, param.table))
      sql.append(s") stored as parquet")
      spark.sql(sql.toString)
    }

    /**
     * get the incremental data's related historical data by their key
     * @param spark SparkSession
     * @param param UpdateParam
     * @param inc the incremental data
     * @return the historical data which related by the incremental data
     */
    def getHitData(spark:SparkSession, param: UpdateParam, inc: DataFrame):DataFrame = {
      inc.createOrReplaceTempView("inc")
      val sql = s"select hit.* from ${param.schema}.${param.table}_hit hit "+
        s"left join inc on hit.${param.keyField}=inc.${param.keyField} "+
        s"where inc.${param.keyField} is null"
      spark.sql(sql).repartition(num)
    }

    /**
     * overwrite the historical table with the incremental data
     * @param spark SparkSession
     * @param param UpdateParam
     * @param inc the incremental data
     * @param hit the historical data
     */
    def overwriteHitTable(spark:SparkSession, param: UpdateParam, inc: DataFrame, hit: DataFrame): Unit = {
      val data = if(hit.isEmpty) inc else inc.union(hit)
      data.createOrReplaceTempView("tmp")
      spark.sql(s"insert overwrite table ${param.schema}.${param.table}_hit "+
        s"select * from tmp").repartition(num)
    }

    val inc = getIncData(spark, param)
    createHitTable(spark, param)
    val hit = getHitData(spark, param, inc)
    overwriteHitTable(spark, param, inc, hit)
  }

  /**
   * update a partition table
   * @param spark SparkSession
   * @param param UpdateParam
   */
  def updatePartitionTable(spark:SparkSession, param:UpdateParam): Unit = {
    val num = param.numPartitions
    def createPartitionTempTable(spark:SparkSession, param:UpdateParam): Unit = {
      spark.sql(s"drop table if exists ${param.schema}.${param.table}_tmp")
      val sql = new StringBuilder
      sql.append(s"create table if not exists ${param.schema}.${param.table}_tmp (")
      sql.append(getDescription(spark, param.schema, param.table))
      sql.append(s") partitioned by (${param.partitionField} ${param.partitionType}) stored as parquet")
      spark.sql(sql.toString)
    }

    def insertIncIntoTempTable(spark:SparkSession, param:UpdateParam): Unit = {
      spark.sql("set hive.exec.dynamic.partition=true")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      spark.sql(s"insert overwrite table ${param.schema}.${param.table}_tmp "+
        s"partition (${param.partitionField}) "+
        s"select *,${param.partedField} as ${param.partitionField} "+
        s"from ${param.schema}.${param.table} distribute by ${param.partedField}")
        .repartition(num)
    }

    def createHitTable(spark:SparkSession, param:UpdateParam): Unit = {
      val isExists = spark.sql(s"show tables in ${param.schema} like '${param.table}_hit'")
      if(!isExists.isEmpty) return
      val sql = new StringBuilder
      sql.append(s"create table if not exists ${param.schema}.${param.table}_hit (")
      sql.append(getDescription(spark, param.schema, param.table))
      sql.append(s") partitioned by (${param.partitionField} ${param.partitionType}) stored as parquet")
      spark.sql(sql.toString)
    }

    def getHitDataInOnePartition(spark:SparkSession, param:UpdateParam, partitionKey: Int) = {
      val sql = s"select hit.* from ${param.schema}.${param.table}_hit hit "+
        s"left join ${param.schema}.${param.table}_tmp inc "+
        s"on hit.${param.keyField}=inc.${param.keyField} "+
        s"where inc.${param.keyField} is null and hit.${param.partitionField}=${partitionKey}"
      spark.sql(sql).repartition(num)
    }

    def overwriteHitInOnePartition(spark:SparkSession, param:UpdateParam, partitionKey: Int, hit: DataFrame) = {
      val incSql = s"select * from ${param.schema}.${param.table}_tmp " +
        s"where ${param.partitionField}=${partitionKey}"
      val inc = spark.sql(incSql).repartition(num)
      val data = if(hit.isEmpty) inc else inc.union(hit)
      data.createOrReplaceTempView("tmp")

      val columns = getColumns(spark, param.schema, param.table)
      val sql = s"insert overwrite table ${param.schema}.${param.table}_hit " +
        s"partition(${param.partitionField}=$partitionKey) " +
        s"select $columns from tmp "
      spark.sql(sql).repartition(num)
    }

    def overwriteHitTable(spark:SparkSession, param:UpdateParam): Unit = {
      import spark.implicits._
      val partitionKeys = spark.sql(s"select distinct ${param.partitionField} " +
        s"from ${param.schema}.${param.table}_tmp order by ${param.partitionField}")
        .repartition(num)
        .map(x=>x(0).toString.toInt).collect
      partitionKeys.foreach(partitionKey => {
        val hit = getHitDataInOnePartition(spark, param, partitionKey)
        overwriteHitInOnePartition(spark, param, partitionKey, hit)
      })
    }

    createPartitionTempTable(spark, param)
    insertIncIntoTempTable(spark, param)
    createHitTable(spark, param)
    overwriteHitTable(spark, param)
  }

  def getDescription(spark:SparkSession, schema: String, table: String): String = {
    import spark.implicits._
    val sql = new StringBuilder
    val desc = spark.sql(s"desc $schema.$table")
    desc.map(x=>x(0).toString+" "+x(1).toString+",").collect.foreach(sql.append)
    // remove the last ','
    sql.deleteCharAt(sql.length-1)
    sql.toString()
  }

  def getColumns(spark:SparkSession, schema: String, table: String): String = {
    import spark.implicits._
    val sql = new StringBuilder
    val desc = spark.sql(s"desc $schema.$table")
    desc.map(x=>x(0).toString+",").collect.foreach(sql.append)
    // remove the last ','
    sql.deleteCharAt(sql.length-1)
    sql.toString()
  }
}
