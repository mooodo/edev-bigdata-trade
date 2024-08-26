package com.edev.bigdata.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * The hdfs utility
 * @author Fangang
 */
object FileUtils {
  
  /**
   * @param pathStr the path
   * @return decide whether the file is exists
   */
  def isExists(pathStr: String): Boolean = {
    val config = new Configuration
    val fs = FileSystem.get(config)
    val path = new Path(pathStr)
    fs.exists(path)
  }
  /**
   * remove hdfs file by path
   * @param pathStr the path
   * @return true when delete success, false otherwise
   */
  def remove(pathStr: String): Boolean ={
    val config = new Configuration
    val fs = FileSystem.get(config)
    val path = new Path(pathStr)
    fs.delete(path, true)
  }
}