package dps.mission

import java.net.InetAddress

import scala.util.Random

import org.apache.commons.math3.random.RandomDataGenerator

object Tool {
  val num: Array[String] = Array("189000")
  val math3lib = new RandomDataGenerator()
  def randomNumArea(): String = {
    num.apply(random(0, num.length-1))
  }
  def randomMsdn(): String = {
    randomNumArea() + randomString(5, "0,1,2,3,4,5,6,7,8,9")
  }
  def random(min: Int, max: Int): Int = {
    Random.nextInt(max - min + 1) + min
  }
  def randomString(length: Int): String = {
    randomString(length, "0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z")
  }
  def randomString(length: Int, sample: String): String = {
    val randomString = new StringBuffer();
    val sampleSplit = sample.split(",")
    for (i <- 0 until length) {
      val index = random(0, sampleSplit.size - 1);
      randomString.append(sampleSplit.apply(index))
    }
    randomString.toString()
  }
  def randomLong(min: Long, max: Long): Long = {
    math3lib.nextLong(min, max)
  }
  def ip2Int(ip:String):Int={
    val bytes = InetAddress.getByName(ip).getAddress
    bytes(3) & 0xFF | (bytes(2) << 8) & 0xFF00 | (bytes(1) << 16) & 0xFF0000 | (bytes(0) << 24) & 0xFF000000

  }
  def int2Ip(ip:Int):String={
    s"${(ip >> 24) & 0xff}.${(ip >> 16) & 0xff}.${(ip >> 8) & 0xff}.${ip & 0xff}"
  }
  def randomIpNum():Int={
    val ip = random(1, 254)+"."+random(1, 254)+"."+random(1, 254)+"."+random(1, 254)
    return ip2Int(ip);
  }
}