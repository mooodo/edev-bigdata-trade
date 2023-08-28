package com.edev.flink.trade.userBehavior

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object SaveToHiveStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    UserBehaviorTable.createTable(tEnv)

    val name = "hive"
    val defaultDatabase = "default"
    val hiveConf = "/usr/local/hive/conf"
    val hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConf)
    tEnv.registerCatalog(name, hiveCatalog)
    //tEnv.useCatalog(name)
    //tEnv.useDatabase("etl")
    tEnv.executeSql(
      s"""
        |insert into $name.etl.t_user_behavior
        |select * from user_behavior
        |""".stripMargin)
  }
}
