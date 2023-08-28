package com.edev.flink.trade.userBehavior

case class LoggingData(date: String, time: String, level: String, thread: String,
                       logger: String, message: String)
