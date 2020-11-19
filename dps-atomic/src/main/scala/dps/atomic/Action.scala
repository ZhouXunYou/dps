package dps.atomic
import scala.collection.mutable.Map
trait Action {
    def doIt(params: Map[String, String]): Any
//    def getInputType[T]:Class[T]
//    def getOutputType[T]:Class[T]
}