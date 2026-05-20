package com.edev.flink.trade.userBehavior

import com.edev.flink.utils.PropertiesUtils
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object UserBehaviorTable {
  def createTable(tEnv: StreamTableEnvironment): Unit = {
    val startupMode = PropertiesUtils.get("scan.startup.mode", "earliest-offset")
    val bootstrapServers = PropertiesUtils.get("bootstrap.servers", "localhost:9092")
    tEnv.executeSql(
      s"""
        |CREATE TABLE user_behavior (
        |    ts STRING,
        |    `user` STRING,
        |    `token` STRING,
        |    `ip` STRING,
        |    `method` STRING,
        |	   `uri` STRING
        |) WITH (
        |    'connector' = 'kafka',  -- using kafka connector
        |    'topic' = 'userBehavior',  -- kafka topic
        |    'scan.startup.mode' = '$startupMode',  -- reading from the beginning
        |    'properties.bootstrap.servers' = '$bootstrapServers',  -- kafka broker address
        |    'format' = 'json'  -- the data format is json
        |);
        |""".stripMargin)
  }

  def saveUserBehavior(tEnv: StreamTableEnvironment): Unit = {
    tEnv.createTemporaryFunction("decodeUserBehavior", new decodeUserBehavior)
    tEnv.executeSql(
      """
        |insert into user_behavior
        |select CONCAT(`date`, ' ', `time`) as ts, `user`, `token`, `ip`, `method`, `uri`
        |from logging_data,
        |LATERAL TABLE(decodeUserBehavior(message)) as t(`user`, `token`, `ip`, `method`, `uri`)
        |""".stripMargin)
  }

  @FunctionHint(output = new DataTypeHint("ROW<user STRING, token STRING, ip STRING, method STRING, uri STRING>"))
  private class decodeUserBehavior extends TableFunction[Row] {
    def eval(message: String): Unit = {
      val array = message.split(",")
      if (array.length < 5) return
      val user = array(0)
      val token = array(1)
      val ip = array(2)
      val method = array(3)
      val uri = array(4)
      collect(Row.of(user, token, ip, method, uri))
    }
  }
}
