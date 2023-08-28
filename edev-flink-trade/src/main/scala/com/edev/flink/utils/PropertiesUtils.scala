package com.edev.flink.utils

import java.util.Properties

object PropertiesUtils {
  private var properties:Properties = new Properties()
  def read(): Unit = {
    val props = new Properties()
    val is = this.getClass.getClassLoader.getResourceAsStream("kafka.properties")
    props.load(is)
    properties = props
  }
  def set(propertyName: String, value: String): Unit = {
    properties.setProperty(propertyName, value)
  }
  def get(propertyName: String): String = {
    properties.getProperty(propertyName)
  }
  def get(propertyName: String, default: String): String = {
    val value = get(propertyName)
    if(value!=null) value else default
  }
  def getAll(): Properties = {
    properties
  }
}
