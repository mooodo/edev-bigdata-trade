package com.edev.bigdata.utils

import scala.collection.mutable

class SaveConf {
  val conf: mutable.Map[String, String] = mutable.Map[String, String]()
  def option(key : String, value : String): SaveConf = {
    conf(key) = value
    this
  }
  def get(key : String): String = {
    conf(key)
  }
  def getAllConfigs: mutable.Map[String, String] = {
    conf
  }
}

object SaveConf {
  def build(): SaveConf = {
    new SaveConf
  }
}
