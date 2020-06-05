package dps.utils
import scala.collection.mutable.Map
import scala.util.control.Breaks

object RunParam {
  def parserArgements(args:Array[String]):Map[String,String]={
    val paramKeys = Map[String, String]()
    for (i <- Range(0, args.length, 2)) {
      paramKeys.put(args.apply(i), args.apply(i + 1))
    }
    paramKeys
  }
  def validRequiredArgements(params:Map[String,String],requiredKeys:Seq[String]):Boolean={
    var count = 0
    requiredKeys.foreach(key=>{
      if(params.contains(key)){
        count = count + 1
      }
    })
    return requiredKeys.length == count
  }
}