package dps.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtils {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  def output(value: Any): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }
}