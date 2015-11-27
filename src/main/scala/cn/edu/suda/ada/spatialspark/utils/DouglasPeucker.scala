package cn.edu.suda.ada.spatialspark.utils

import cn.edu.suda.ada.spatialspark.core.{Trajectory, Point, GPSPoint}

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of Douglas-Peucker algorithm
 * This is used to simpify the original trajectory
 * Created by liyang
 */
object DouglasPeucker{
  def reduction(traj :Trajectory,bound:Double= 50):Trajectory = {
    val gPSPoint = traj.GPSPoints
    val list = reduction(gPSPoint.toArray,bound)
    traj.GPSPoints = list
    traj
  }
   def reduction(GPSPoints : Array[GPSPoint],bound:Double ):List[GPSPoint] = {
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
   def compress(GPSPoints : Array[GPSPoint],low: Int,high:Int,bound: Double):Array[GPSPoint] = {
    if(low < high -1){                                          //If there are more than 3 point including the first and last points, else return empty Array
      val first = GPSPoints(low)
      val last = GPSPoints(high)
      var splitPoints: ArrayBuffer[GPSPoint] = ArrayBuffer[GPSPoint]()
      val distance = getDistance(first,last)   //Calculate the distance between the first and the last points. This would be used in latter iterations
      var maxHeight = 0.0
      var index = low + 1

      for (i <- low+1 until high) {                                   //Iterate over the points to get the max height
        val height = getVerticalHeight(first,GPSPoints(i) , last, distance)
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
   * @return distance in kilometer
   */
  private def getDistance(p1: GPSPoint,p2:GPSPoint):Double = p1.getDistance(p2)


  private def getVerticalHeight(first: GPSPoint,p2:GPSPoint,last:GPSPoint,first2Last:Double):Double = {
     val edge12 = first.getDistance(p2)
     val edge23 = last.getDistance(p2)
     val edge13 = first2Last
     val p = (edge12 + edge13 + edge23)

    val area = Math.sqrt(p*(p-edge12)*(p-edge23)*(p-edge13))
    2000*area/first2Last
  }
}
