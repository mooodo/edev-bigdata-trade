package com.edev.flink.trade.userBehavior

import com.edev.flink.utils.PropertiesUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object LoggingDataTable {
  def createTable(tEnv: StreamTableEnvironment): Unit = {
    val startupMode = PropertiesUtils.get("scan.startup.mode", "earliest-offset")
    val bootstrapServers = PropertiesUtils.get("bootstrap.servers", "localhost:9092")
    tEnv.executeSql(
      s"""
        |CREATE TABLE logging_data (
        |    `date` STRING,
        |    `time` STRING,
        |    `level` STRING,
        |    `thread` STRING,
        |    `logger` STRING,
        |	   `message` STRING
        |) WITH (
        |    'connector' = 'kafka',  -- using kafka connector
        |    'topic' = 'loggingData',  -- kafka topic
        |    'scan.startup.mode' = '$startupMode',  -- reading from the beginning
        |    'properties.bootstrap.servers' = '$bootstrapServers',  -- kafka broker address
        |    'format' = 'json'  -- the data format is json
        |);
        |""".stripMargin)
  }

  def saveToTable(tEnv: StreamTableEnvironment, loggingData: DataStream[LoggingData]): Unit = {
    val vTable = tEnv.fromDataStream(loggingData)
    tEnv.createTemporaryView("vLoggingData", vTable)
    tEnv.executeSql(
      """
        |insert into logging_data
        |select * from vLoggingData
        |""".stripMargin)
  }
}
