package dps.atomic.util


object LatLngUtil {
  def check(point: Point, points: Array[Point]):Boolean = {
    if (points.length < 3) {
      return false
    }
    var peneralPath = new java.awt.geom.GeneralPath();
    peneralPath.moveTo(points.apply(0).lng, points.apply(0).lat);
    for (i <- 1 to points.length) {
        peneralPath.lineTo(points.apply(i).lng, points.apply(i).lat);
    }
    peneralPath.lineTo(points.apply(0).lng, points.apply(0).lat);
    peneralPath.closePath();
    
    peneralPath.contains(new java.awt.geom.Point2D.Double(point.lng,point.lat))
  }
  def inPoly(point: Point, points: Array[Point]): Boolean = {
    if (points.length < 3) {
      return false
    }
    var sum = 0
    var dLon1: Double = 0.0f
    var dLon2: Double = 0.0f
    var dLat1: Double = 0.0f
    var dLat2: Double = 0.0f
    var dLon: Double = 0.0f
    for (i <- 0 to points.length) {
      if (i == points.length - 1) {
        dLon1 = points.apply(i).lng
        dLat1 = points.apply(i).lat;
        dLon2 = points.apply(0).lng;
        dLat2 = points.apply(0).lat;
      } else {
        dLon1 = points.apply(i).lng;
        dLat1 = points.apply(i).lat;
        dLon2 = points.apply(i + 1).lng;
        dLat2 = points.apply(i + 1).lat;
      }
      //以下语句判断A点是否在边的两端点的水平平行线之间，在则可能有交点，开始判断交点是否在左射线上
      if (((point.lat >= dLat1) && (point.lat < dLat2)) || ((point.lat >= dLat2) && (point.lat < dLat1))) {
        if (Math.abs(dLat1 - dLat2) > 0) {
          //得到 A点向左射线与边的交点的x坐标：
          dLon = dLon1 - ((dLon1 - dLon2) * (dLat1 - point.lat)) / (dLat1 - dLat2);
          if (dLon < point.lng) {
            sum = sum + 1
          }
        }
      }
    }
    sum % 2 != 0
  }
}