package dps.mission.log

import org.apache.log4j.Level
object LogPrinter {
  def print(clazz:Class[Any],content:String,level:Level): Unit = {
    val logger = org.apache.log4j.Logger.getLogger(clazz)
    if(logger.isEnabledFor(level)){
      logger.log(level, content)
    }
  }
}