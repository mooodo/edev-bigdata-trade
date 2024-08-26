package com.edev.bigdata.utils

import scala.collection.mutable

class HudiConf {
  val conf: mutable.Map[String, String] = mutable.Map[String, String]()
  def option(key : String, value : String): HudiConf = {
    conf(key) = value
    this
  }
  def getAllConfigs: mutable.Map[String, String] = {
    conf
  }
}

object HudiConf {
  def build(): HudiConf = {
    new HudiConf
  }
}
