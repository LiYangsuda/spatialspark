package cn.edu.suda.ada.spatialspark.filters

import cn.edu.suda.ada.spatialspark.core.{Range, Trajectory}

/**
 * Created by liyang on 15-9-4.
 */
trait TrajectoryFilter extends Serializable{
  def doFilter(trajectory:Trajectory):Boolean
  override def toString: String = "TrajectoryFilter"
}

/**
 * Filter on a single trajectory based on OTime
 */

object TrajectoryOTimeFilter extends TrajectoryFilter{
  var otime: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.otime = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    if(relation == "" || relation.equals("")) throw new Exception("relation is not defined in"+this.toString)
    val t = trajectory.getStarTime

    val result: Boolean = relation match {
      case "gt" => t > otime
      case "lt" => t < otime
      case "equal" => t == otime
      case "ngt" => t <= otime
      case "nlt" => t >= otime
      case _ => throw new IllegalArgumentException("filter relation must be one of: (1) gt (>) (2) lt (<) (3) equal (=) (4) ngt (<=) (5) nlt (>=)")
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryOTimeFilter: otime = "+otime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on DTime
 */

object TrajectoryDTimeFilter extends TrajectoryFilter{
  var dtime: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.dtime = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val t = trajectory.getEndTime

    val result: Boolean = relation match {
      case "gt" => t > dtime
      case "lt" => t < dtime
      case "equal" => t == dtime
      case "ngt" => t <= dtime
      case "nlt" => t >= dtime
      case _ => throw new IllegalArgumentException("filter relation must be one of: (1) gt (>) (2) lt (<) (3) equal (=) (4) ngt (<=) (5) nlt (>=)")
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryDTimeFilter: dtime = "+dtime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory
 */

object TrajectoryTravelTimeFilter extends TrajectoryFilter{
  var traveltime: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.traveltime = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val ttime = trajectory.getDuration

    val result: Boolean = relation match {
      case "gt" => ttime > traveltime
      case "lt" => ttime < traveltime
      case "equal" => ttime == traveltime
      case "ngt" => ttime <= traveltime
      case "nlt" => ttime >= traveltime
      case _ => throw new IllegalArgumentException("filter relation must be one of: (1) gt (>) (2) lt (<) (3) equal (=) (4) ngt (<=) (5) nlt (>=)")
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryTravelTimeFilter: traveltime = "+traveltime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory travel distance
 */
//}
object TrajectoryTravelDistanceFilter extends TrajectoryFilter{
  var travelDistance: Float = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param dis  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(dis : Float,relation:String): Unit ={
    this.travelDistance = dis
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val distance = trajectory.getTravelDistance

    val result: Boolean = relation match {
      case "gt" => distance > travelDistance
      case "lt" => distance < travelDistance
      case "equal" => distance == travelDistance
      case "ngt" => distance <= travelDistance
      case "nlt" => distance >= travelDistance
      case _ => throw new IllegalArgumentException("relation in" +this + "is"+relation+
        "filter relation must be one of: (1) gt (>) (2) lt (<) (3) equal (=) (4) ngt (<=) (5) nlt (>=)")
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryTravelDistanceFilter: travelDistance = "+travelDistance + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

object TrajectoryAvgSpeedFilter extends TrajectoryFilter{
  var averageSpeed: Float = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param avg  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(avg: Float,relation:String): Unit ={
    this.averageSpeed = avg
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {

    val avgSpeed = trajectory.getAverageSpeed
//Use pattern matching to simplify the code above
    val result: Boolean = relation match {
      case "gt" => avgSpeed > averageSpeed
      case "lt" => avgSpeed < averageSpeed
      case "equal" => avgSpeed == averageSpeed
      case "ngt" => avgSpeed <= averageSpeed
      case "nlt" => avgSpeed >= averageSpeed
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryAvgSpeedFilter: averageSpeed = "+averageSpeed + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

object TrajectoryAvgSampleTimeFilter extends TrajectoryFilter{
  var averageSampleTime: Float = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param interval  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(interval:Float,relation:String): Unit ={
    this.averageSampleTime = interval
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val avgInterval = trajectory.getAverageSampleInterval
//
//    var result: Boolean = true
//    if(relation == "gt") {result = if(avgInterval > averageSampleTime) true else false}
//    if(relation == "lt") {result = if(avgInterval < averageSampleTime) true else false}
//    if(relation == "equal") {result = if(avgInterval == averageSampleTime) true else false}
//    if(relation == "ngt") {result = if(avgInterval <= averageSampleTime) true else false}
//    if(relation == "nlt") {result = if(avgInterval >= averageSampleTime) true else false}
//    result
    val result: Boolean = relation match {
      case "gt" => avgInterval > averageSampleTime
      case "lt" => avgInterval < averageSampleTime
      case "equal" => avgInterval == averageSampleTime
      case "ngt" => avgInterval <= averageSampleTime
      case "nlt" => avgInterval >= averageSampleTime
      case _ => throw new IllegalArgumentException("filter relation must be one of: (1) gt (>) (2) lt (<) (3) equal (=) (4) ngt (<=) (5) nlt (>=)")
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryAvgSampleTimeFilter: otime = "+averageSampleTime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on range of the start point
 */

object TrajectoryOPointFilter extends TrajectoryFilter{
  var range: Range = null
  def setParameters(range: Range): Unit ={
    this.range = range
  }
  def doFilter(trajectory:Trajectory):Boolean = {
    val p = trajectory.getStartPoint
    range.contains(p.getPoint())
  }
  override def toString = "Object TrajectoryOPointFilter: range = "+range.toString
}
/**
 * Filter on a single trajectory based on range of the end point
 */
object TrajectoryDPointFilter extends TrajectoryFilter {
  var range: Range = null
  def setParameters(range:Range): Unit ={
    this.range = range
  }

  override def doFilter(trajectory: Trajectory): Boolean = {
    val p = trajectory.getEndPoint

    range.contains(p.getPoint())
  }
  override def toString = "TrajectoryDPointFilter: range = "+range.toString
}
/**
 * Filter on a single trajectory based on whether the trajectory pass over a particular area
 */
object TrajectoryPassRangeFilter extends TrajectoryFilter{
  var range: Range =null
  def setParameters(range: Range): Unit ={
    this.range = range
  }

  /**
   * Test whether there exists a sample point in the passed in Range.
   * @note If the sample interval is large, this method needs to be reconstruct
   * @param trajectory
   * @return Return true if the trajectory passes by the given area, otherwise false.
   */
  override def doFilter(trajectory: Trajectory): Boolean = {
    trajectory.GPSPoints.exists(point => range.contains(point.getPoint()))
  }

  override def toString = "Object TrajectoryPassRangeFilter: range = "+range.toString
}

//object GPSPointRangeFilter extends TrajectoryFilter{
//  var range : Range = null
//  def setParameters(range: Range): Unit ={
//    this.range = range
//  }
//
////  override def doFilter(trajectory: Trajectory): Trajectory = {
////
////  }
//}