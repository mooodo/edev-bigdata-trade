package com.edev.flink.trade.userBehavior

import com.edev.flink.utils.PropertiesUtils
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object LoggingDataTable {
  def createLoggingTable(tEnv: StreamTableEnvironment): Unit = {
    val startupMode = PropertiesUtils.get("scan.startup.mode", "earliest-offset")
    val bootstrapServers = PropertiesUtils.get("bootstrap.servers", "localhost:9092")
    tEnv.executeSql(
      s"""
         |CREATE TABLE logging (
         |    `line` STRING
         |) WITH (
         |    'connector' = 'kafka',  -- using kafka connector
         |    'topic' = 'log4j',  -- kafka topic
         |    'scan.startup.mode' = '$startupMode',  -- reading from the beginning
         |    'properties.bootstrap.servers' = '$bootstrapServers',  -- kafka broker address
         |    'format' = 'csv'  -- the data format is json
         |);
         |""".stripMargin)
  }
  def createLoggingDataTable(tEnv: StreamTableEnvironment): Unit = {
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

  def saveLoggingData(tEnv: StreamTableEnvironment): Unit = {
    tEnv.createTemporaryFunction("decodeLogging", new decodeLogging)
    tEnv.executeSql(
      """
        |insert into logging_data
        |select `date`, `time`, `level`, `thread`, `logger`, `message`
        |from logging,
        |LATERAL TABLE(decodeLogging(line)) as t(`date`, `time`, `level`, `thread`, `logger`, `message`)
        |""".stripMargin)
  }

  @FunctionHint(output = new DataTypeHint("ROW<date STRING, time STRING, level STRING, thread STRING, logger STRING, message STRING>"))
  private class decodeLogging extends TableFunction[Row] {
    def eval(line: String): Unit = {
      val strArray = line.split("\\s")
      if(strArray.length <= 5) return
      val date = strArray(0)
      val time = strArray(1)
      val level = strArray(2)
      val thread = strArray(3)
      val logger = strArray(4)
      var message = ""
      for(i<- 5 until strArray.size) message = message+strArray(i)
      collect(Row.of(date, time, level, thread, logger, message))
    }
  }
}
