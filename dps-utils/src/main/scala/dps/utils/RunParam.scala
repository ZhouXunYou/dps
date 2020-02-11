package data.process.util
import scala.collection.mutable.Map
object RunParam {
  def parserArgements(args:Array[String]):Map[String,String]={
    val paramKeys = Map[String, String]()
    for (i <- Range(0, args.length, 2)) {
      paramKeys.put(args.apply(i), args.apply(i + 1))
    }
    paramKeys
  }
}