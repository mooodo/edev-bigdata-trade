package com.edev.bigdata.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * The data utility
 * @author Fangang
 */
object DateUtils {
  def getNow = {
    val dateFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(new Date())
  }
  def getToday = {
    val dateFormat =  new SimpleDateFormat("yyyyMMdd")
    dateFormat.format(new Date())
  }
  def getThisMonth = {
    val dateFormat =  new SimpleDateFormat("yyyyMM")
    dateFormat.format(new Date())
  }
  def getNextDay(date: Date) ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, 1)
    calendar.getTime
  }
  
  def getNextMonth(date: Date) ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, 1)
    calendar.getTime
  }
  
  def addMonth(date: Date, month: Int) ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, month)
    calendar.getTime
  }
  def addHour(date: Date, hour: Int) ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.HOUR, hour)
    calendar.getTime
  }
  def getDatesBetween(start: Date, end: Date, dateList: List[Date]):List[Date]  = {
    if(start.after(end)) dateList
    else getDatesBetween(getNextDay(start), end, dateList.:+(start))
  }
  
  def getMonthsBetween(start: Date, end: Date, dateList: List[Date]):List[Date]  = {
    if(start.after(end)) dateList
    else getMonthsBetween(getNextMonth(start), end, dateList.:+(start))
  }
  
  def format(date: Date, format: String) = {
    val dateFormat =  new SimpleDateFormat(format)
    if(date==null) null
    else dateFormat.format(date)
  }
  
  def getTime(strTime:String):Date = {
    val format = if(strTime.length()==10) "yyyy-MM-dd" else "yyyy-MM-dd HH:mm:ss"
    getTime(strTime, format)
  }
  
  def getTime(strTime:String, format:String) = {
    val dateFormat =  new SimpleDateFormat(format)
    try{
      dateFormat.parse(strTime)
    }catch {
      case ex:Exception  => {
        throw new RuntimeException("error date format! The right format is: "+format,ex)
      }}
  }
  
  def getLastDaysOfMonth(date: String, num: Int, format:String) = {
    val cal = Calendar.getInstance
    val dateFormat =  new SimpleDateFormat(format)

    val year = date.substring(0,4).toInt
    val month = date.substring(5,7).toInt

    cal.set(year,month - 1,1)
    cal.roll(Calendar.DATE,-num)
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val period = df.format(cal.getTime)
    dateFormat.parse(period.toString)
  }
}