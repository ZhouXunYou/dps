package dps.datasource


abstract class DataSource(val params:Map[String,String]) {
   def read(): Any
}