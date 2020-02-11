package dps.generator

import data.process.util.SessionOperation
import dps.utils.JsonUtils
import dps.atomic.model.Mission
import com.fasterxml.jackson.module.scala.JacksonModule
import com.fasterxml.jackson.databind.Module.SetupContext

object Test {
  def main(args: Array[String]): Unit = {
    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99:5432/dps", "postgres", "postgres")
    val ml = new MissionLoader(so)
    val mission = ml.getMission("test")
//    val sg = new SourceGenerator(mission)
//    sg.produce("d:\\outfile")
    println(JsonUtils.output(mission))
    
//    val mapper = new ObjectMapper() with ScalaObjectMapper
//    mapper.registerModule(DefaultScalaModule)
//    val m = Map("a"->"b","c"->"d")
//    println(mapper.writeValueAsString(m))
    
  }
}