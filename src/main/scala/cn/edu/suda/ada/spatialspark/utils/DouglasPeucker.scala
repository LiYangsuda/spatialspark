package cn.edu.suda.ada.spatialspark.utils

import cn.edu.suda.ada.spatialspark.core.{Point, GPSPoint}

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of Douglas-Peucker algorithm
 * This is used to simpify the original trajectory
 * Created by liyang
 */
object DouglasPeucker{
  def DouglasPeuckerReduction(GPSPoints : Array[GPSPoint],bound:Float):List[GPSPoint] = {
    val first = GPSPoints.head
    val last = GPSPoints.last
    var points = ArrayBuffer[GPSPoint]()
    points += first
    val middle = compress(GPSPoints,0,GPSPoints.length-1,bound)
    points = points ++ middle
    points += last
    points.toList
  }
  /**
   * Compress the data using Douglas-Peucker algorithm.
   * @param GPSPoints Points to be compressed
   * @param low  low bound
   * @param high  upper bound
   * @param bound Geographic coordinates offset. Usually, this should be less than 0.00001 to reflect distance of meter
   * @return
   */
  def compress(GPSPoints : Array[GPSPoint],low: Int,high:Int,bound: Double):Array[GPSPoint] = {

    val first = GPSPoints(low)
    val last = GPSPoints(high)
    var splitPoints: ArrayBuffer[GPSPoint] = ArrayBuffer[GPSPoint]()
    val distance = getDistance(first.getPoint(),last.getPoint())
    var maxvariation = 0.0
      var index = 1
    if(low < high -1){
      for (i <- low + 1 to high - 1) {
        val height = getVerticalDistance(GPSPoints(i), first, last, distance)
        if (height > maxvariation) {
          maxvariation = height
          index = i
        }
      }
      if(maxvariation == 0.0)
        println("low:"+low+"  high"+high)
      if (maxvariation > bound) {
        splitPoints = splitPoints ++ compress(GPSPoints, low, index, bound)
        splitPoints += GPSPoints(index)
        splitPoints = splitPoints ++ compress(GPSPoints, index, high, bound)
      }
    }
    splitPoints.toArray
  }

  /**
   * Get the distance between two points
   * @param p1
   * @param p2
   * @return
   */
  private def getDistance(p1: Point,p2:Point):Double = Math.sqrt((p1.x - p2.x)*(p1.x - p2.x)+(p1.y - p2.y)*(p1.y - p2.y))

  /**
   * //Area = |(1/2)(x1y2 + x2y3 + x3y1 - x2y1 - x3y2 - x1y3)|   *Area of triangle
   * //Base = v((x1-x2)²+(x1-x2)²)                               *Base of Triangle*
   * //Area = .5*Base*H                                          *Solve for height
   * //Height = Area/.5/Base
   */
  def getVerticalDistance(p1: GPSPoint,p2:GPSPoint,p3:GPSPoint,first2Last:Double):Double = {
    val area =Math.abs(p1.longitude*p2.latitude + p2.longitude*p3.latitude + p3.longitude*p1.latitude -
      p2.longitude*p1.latitude - p3.longitude*p2.latitude - p1.longitude*p3.latitude)

    val height = area / first2Last
    height
  }
}
