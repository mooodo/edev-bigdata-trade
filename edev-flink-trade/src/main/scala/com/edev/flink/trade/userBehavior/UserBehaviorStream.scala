package com.edev.flink.trade.userBehavior

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object UserBehaviorStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    LoggingDataTable.createTable(tEnv)
    val table = tEnv.from("logging_data")
    val userBehaviors = tEnv.toDataStream(table)
      .map(row => buildUserBehavior(row))
    UserBehaviorTable.createTable(tEnv)
    UserBehaviorTable.saveToTable(tEnv, userBehaviors)
    env.execute("Collect user behaviors")
  }

  def buildUserBehavior(row: Row): UserBehavior = {
    val ts = row.getField("date") + " " + row.getField("time")
    val message = row.getField("message")
    if (message == null) return UserBehavior(ts, null, null, null, null, null)
    val array = message.toString.split(",")
    if (array.length < 5) return UserBehavior(ts, null, null, null, null, null)
    val user = array(0)
    val token = array(1)
    val ip = array(2)
    val method = array(3)
    val uri = array(4)
    UserBehavior(ts, user, token, ip, method, uri)
  }
}
