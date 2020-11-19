package dps.atomic
import scala.collection.mutable.Map
trait Action {
    def doIt(params: Map[String, String]): Any
}