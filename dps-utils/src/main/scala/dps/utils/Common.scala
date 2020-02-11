package data.process.util

object Common {
  def getHumpName(name: String): String = {
    if(name.length()>1){
      val firstChar = name.substring(0, 1)
      val otherChars = name.substring(1)
      s"${firstChar.toUpperCase()}${otherChars}"
    }else{
      name.toUpperCase()
    }
  }
}