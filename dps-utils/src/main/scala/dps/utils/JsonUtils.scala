package dps.utils

import com.fasterxml.jackson.databind.ObjectMapper

object JsonUtils {
  val om = new ObjectMapper
  def output(value: Any): String = {
    println(om.writeValueAsString(value))
    return om.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }
}