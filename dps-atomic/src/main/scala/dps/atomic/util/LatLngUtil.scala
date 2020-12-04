package dps.atomic.util


object LatLngUtil {
  def main(args: Array[String]): Unit = {
    val point = new Point(116.35105133056642,39.92105925506222)
    val points = Array(new Point(116.35105133056642,39.92105925506222),new Point(116.35105133056642,39.93948807471046),new Point(116.41834259033205,39.93948807471046),new Point(116.41834259033205,39.92105925506222));
    println(LatLngUtil.check(point, points));
    
  }
  def check(point: Point, points: Array[Point]):Boolean = {
    if (points.length < 3) {
      return false
    }
    var generalPath = new java.awt.geom.GeneralPath();
    generalPath.moveTo(points.apply(0).lng, points.apply(0).lat);
    for (i <- 1 to points.length-1) {
        generalPath.lineTo(points.apply(i).lng, points.apply(i).lat);
    }
    generalPath.lineTo(points.apply(0).lng, points.apply(0).lat);
    generalPath.closePath();
    
    generalPath.contains(new java.awt.geom.Point2D.Double(point.lng,point.lat))
  }
}