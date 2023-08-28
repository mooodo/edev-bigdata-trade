package com.edev.flink.trade.userBehavior

import com.edev.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.Properties

object LoggingDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    PropertiesUtils.read()
    if(args.length>0) PropertiesUtils.set("bootstrap.servers",args(0))
    val loggingData = readLogging(env)

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    LoggingDataTable.createTable(tEnv)
    LoggingDataTable.saveToTable(tEnv, loggingData)
    env.execute("Read logging data from kafka")
  }

  def readLogging(env: StreamExecutionEnvironment): DataStream[LoggingData] = {
    val topic = PropertiesUtils.get("topic")
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), PropertiesUtils.getAll())
    consumer.setStartFromLatest()
    val logging = env.addSource(consumer)
    logging.map(line => decode(line))
  }

  def decode(line: String): LoggingData = {
    val strArray = line.split("\\s")
    if(strArray.length <= 5) {
      var message = ""
      strArray.foreach(s => message=message+s)
      return LoggingData(null,null,null,null,null,message)
    }
    val date = strArray(0)
    val time = strArray(1)
    val level = strArray(2)
    val thread = strArray(3)
    val logger = strArray(4)
    var message = ""
    for(i<- 5 until strArray.size) message = message+strArray(i)
    LoggingData(date, time, level, thread, logger, message)
  }
}
