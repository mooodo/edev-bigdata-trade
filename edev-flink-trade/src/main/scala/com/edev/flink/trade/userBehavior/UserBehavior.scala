package com.edev.flink.trade.userBehavior

case class UserBehavior(ts: String, user: String, token: String, ip: String,
                        method: String, uri: String)
