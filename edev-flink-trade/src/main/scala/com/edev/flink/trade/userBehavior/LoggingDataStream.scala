package com.edev.flink.trade.userBehavior

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object LoggingDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    LoggingDataTable.createLoggingTable(tEnv)
    LoggingDataTable.createLoggingDataTable(tEnv)
    LoggingDataTable.saveLoggingData(tEnv)
    env.execute("Read logging data from kafka")
  }
}
