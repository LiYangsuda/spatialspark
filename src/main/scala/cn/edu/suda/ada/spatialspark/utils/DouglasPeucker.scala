package cn.edu.suda.ada.spatialspark.utils

import cn.edu.suda.ada.spatialspark.core.{Trajectory, Point, GPSPoint}

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of Douglas-Peucker algorithm
 * This is used to simpify the original trajectory
 * Created by liyang
 */
object DouglasPeucker{
  def reduction(traj :Trajectory,bound:Double= 0.01):Trajectory = {
    val gPSPoint = traj.GPSPoints
    val list = reduction(gPSPoint.toArray,bound)
    traj.GPSPoints = list
    traj
  }
  private def reduction(GPSPoints : Array[GPSPoint],bound:Double ):List[GPSPoint] = {
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
   * @param bound Geographic coordinates offset.
   * @return all the split points in the array
   */
  private def compress(GPSPoints : Array[GPSPoint],low: Int,high:Int,bound: Double):Array[GPSPoint] = {
    if(low < high -1){                                          //If there are more than 3 point including the first and last points, else return empty Array
      val first = GPSPoints(low)
      val last = GPSPoints(high)
      var splitPoints: ArrayBuffer[GPSPoint] = ArrayBuffer[GPSPoint]()
      val distance = getDistance(first.getPoint(),last.getPoint())   //Calculate the vertical height. This would be used in latter iterations
      var maxHeight = 0.0
      var index = low + 1

      for (i <- low+1 until high) {                                   //Iterate over the points to get the max height
        val height = getVerticalDistance(GPSPoints(i), first, last, distance)
        if (height > maxHeight) {
          maxHeight = height
          index = i
        }
      }

      if (maxHeight > bound) {
        splitPoints = splitPoints ++ compress(GPSPoints, low, index, bound)
        splitPoints += GPSPoints(index)                                 //Add the dividing point in the result array
        splitPoints = splitPoints ++ compress(GPSPoints, index, high, bound)
      }
      splitPoints.toArray
    }else{
      Array[GPSPoint]()
    }
  }

  /**
   * Get the distance between two points
   * @param p1
   * @param p2
   * @return
   */
  private def getDistance(p1: Point,p2:Point):Double = Math.sqrt((p1.x - p2.x)*(p1.x - p2.x)+(p1.y - p2.y)*(p1.y - p2.y))

   /**
   * Get the vertical height of a point in a triangle. If two points in the triangle have the same coordinate, it should output 0
   * Area = |(1/2)(x1y2 + x2y3 + x3y1 - x2y1 - x3y2 - x1y3)|   *Area of triangle
   * Base = v((x1-x2)²+(x1-x2)²)                               *Base of Triangle*
   * Area = .5*Base*H                                          *Solve for height
   * Height = Area/.5/Base
   * @param p1  point1
   * @param p2 point2
   * @param p3 point3
   * @param first2Last  length of the edge
   * @return  vertical height
   */
  private def getVerticalDistance(p1: GPSPoint,p2:GPSPoint,p3:GPSPoint,first2Last:Double):Double = {
    val area = Math.abs(p1.longitude*p2.latitude + p2.longitude*p3.latitude + p3.longitude*p1.latitude
      - p2.longitude*p1.latitude - p3.longitude*p2.latitude - p1.longitude*p3.latitude)

    val height = area / first2Last
    height
  }
}
