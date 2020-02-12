package dps.generator

import data.process.util.SessionOperation
import dps.utils.JsonUtils
import dps.atomic.model.Mission
import com.fasterxml.jackson.module.scala.JacksonModule
import com.fasterxml.jackson.databind.Module.SetupContext

object Test {
  def main(args: Array[String]): Unit = {
//    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://39.98.141.108:16632/dps", "postgres", "1qaz#EDC")
		val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99/dps", "postgres", "postgres")
    val ml = new MissionLoader(so)
    val mission = ml.getMission("test")
    val sg = new SourceGenerator(mission)
    sg.produce("E:\\workspace\\scala.workspace\\dps\\dps-mission","E:\\workspace\\scala.workspace\\dps\\dps-atomic\\src\\main\\resources")
    println(JsonUtils.output(mission))
    
//    val mapper = new ObjectMapper() with ScalaObjectMapper
//    mapper.registerModule(DefaultScalaModule)
//    val m = Map("a"->"b","c"->"d")
//    println(mapper.writeValueAsString(m))
    
  }
}