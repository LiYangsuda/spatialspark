package cn.edu.suda.ada.spatialspark.core

import scala.math._
/**
 * The class Trajectory is an abstract representation of a trajectory, which basically contains a list
 * of GPS points and an ID that uniquely identifies it. This class provides some basic operations that
 * can be performed to answer queries on a particular trajectory, such as average speed, average sampling
 * frequency and similarity between trajectories .etc.
 * @author Graberial
 */
case class Trajectory(val trajectoryID: String,val carID:String,var GPSPoints:List[GPSPoint]) extends Serializable{
  var travelDistance: Float = -1  //Total length of travel distance

  var range: Range = _ //The minimum rectangle that merely covers this trajectory

  /** @return the timestamp of the first GPSPoint in the trajectory */
  def getStarTime = GPSPoints.head.timestamp

  def getEndTime = GPSPoints.last.timestamp

  /**
   * @return Trajectory travel time
   */
  def getDuration = getEndTime - getStarTime
  /** @return the first GPSPoint in the trajectory */
  def getStartPoint: GPSPoint = GPSPoints.head

  def getEndPoint: GPSPoint = GPSPoints.last

  /**
   * @return travel distance of a trajectory
   * @note 只计算坐标距离不是长度，需要进一步完善
   */
  def getTravelDistance: Float = {
    if (travelDistance != -1)
      travelDistance
    else {
      var sum: Double = 0
      for(index <- 0 to GPSPoints.length - 2){
    //    sum += hypot(GPSPoints(index).latitude - GPSPoints(index+1).latitude,GPSPoints(index).longitude - GPSPoints(index+1).longitude)
        sum += GPSPoints(index).getDistance(GPSPoints(index+1))
      }
      travelDistance = sum.toFloat
      travelDistance
    }
  }

  def getAverageSpeed: Float = getTravelDistance / getDuration

  def getAverageSampleInterval: Float = (getEndTime - getStarTime) / GPSPoints.length

  /**
   * Return how many sample points in this trajectory
   * @return Number of sample points
   */
  def length = GPSPoints.length

  /**
   * Get the sub trajectory giving the time limit and rectangle area boundary. This method will create a new Trajectory object
   * while the original one remain unchanged.
   * @param rect      the minimal rectangle that merely covers the this trajectory
   * @return sub trajectory satisfies the time and pass-by area filter
   */
  def getSubTrajectory(rect: Range): Trajectory = {
//    var subGPSPoints: List[GPSPoint] = Nil
//    for (point <- GPSPoints if rect.contains(point.latitude, point.longitude)) {
//      subGPSPoints = point :: subGPSPoints
//    }
    val subTraj = GPSPoints.filter(p => rect.contains(p.getPoint()))

    new Trajectory(trajectoryID, carID, subTraj)
  }

  def getSubTrajectory(interval: Int): Trajectory = {
    val subGPSPoints : List[GPSPoint] = GPSPoints.filter(_.speed > interval)
    new Trajectory(trajectoryID,carID,subGPSPoints)
  }
  /**
   * @return return the rectangle area that merely covers this trajectory
   */
  def getRange: Range = {
    if (range != null)
      range
    else {
      var top, bottom = GPSPoints.head.latitude
      var left, right = GPSPoints.head.longitude

      for (point <- GPSPoints.tail) {
        if (point.latitude > top) top = point.latitude
        else if (point.latitude < bottom) bottom = point.latitude

        if (point.longitude < left) left = point.longitude
        else if (point.longitude > right) right = point.longitude
      }
      range = new Range(left, top, right, bottom)
      range
    }
  }

  /**
   * Get the nearest distance between a GPS point and a trajectory.
   * d(p,T) = min(p,T.q)
   *         q in T
   * @param point
   * @return
   */
  def getDistance(point: GPSPoint):Double = {
    GPSPoints.maxBy(p => p.getDistance(point)).getDistance(point)
  }
  override def toString = "Trajectory: Id ("+trajectoryID+") numGPSPoints("+GPSPoints.length+")"
}
