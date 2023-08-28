package com.edev.flink.trade.userBehavior

import com.edev.flink.utils.PropertiesUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

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

  def saveToTable(tEnv: StreamTableEnvironment, userBehaviors: DataStream[UserBehavior]): Unit = {
    val vUserBehaviors = tEnv.fromDataStream(userBehaviors)
    tEnv.createTemporaryView("vUserBehaviors", vUserBehaviors)
    tEnv.executeSql(
      """
        |insert into user_behavior
        |select *
        |from vUserBehaviors
        |""".stripMargin)
  }
}
