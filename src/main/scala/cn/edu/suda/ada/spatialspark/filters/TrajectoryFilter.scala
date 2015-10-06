package cn.edu.suda.ada.spatialspark.filters

import cn.edu.suda.ada.spatialspark.core.{Trajectory,Range}

/**
 * Created by liyang on 15-9-4.
 */
trait TrajectoryFilter extends Serializable{
  def doFilter(trajectory:Trajectory):Boolean
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
    var result: Boolean = true
    if(relation == "gt") {result = if(t > otime) true else false}
    if(relation == "lt") {result = if(t < otime) true else false}
    if(relation == "equal") {result = if(t == otime) true else false}
    if(relation == "ngt") {result = if(t <= otime) true else false}
    if(relation == "nlt") {result = if(t >= otime) true else false}
    result
  }
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
    var result: Boolean = true
    if(relation == "gt") {result = if(t > dtime) true else false}
    if(relation == "lt") {result = if(t < dtime) true else false}
    if(relation == "equal") {result = if(t == dtime) true else false}
    if(relation == "ngt") {result = if(t <= dtime) true else false}
    if(relation == "nlt") {result = if(t >= dtime) true else false}
    result
  }
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

    var result: Boolean = true
    if(relation == "gt") {result = if(ttime > traveltime) true else false}
    if(relation == "lt") {result = if(ttime < traveltime) true else false}
    if(relation == "equal") {result = if(ttime == traveltime) true else false}
    if(relation == "ngt") {result = if(ttime <= traveltime) true else false}
    if(relation == "nlt") {result = if(ttime >= traveltime) true else false}
    result
  }
}
/**
 * Filter on a single trajectory based on trajectory travel distance
 */
//}
object TrajectoryTravelDistanceFilter extends TrajectoryFilter{
  var travelDistance: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.travelDistance = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val distance = trajectory.getTravelDistance

    var result: Boolean = true
    if(relation == "gt") {result = if(distance > travelDistance) true else false}
    if(relation == "lt") {result = if(distance < travelDistance) true else false}
    if(relation == "equal") {result = if(distance == travelDistance) true else false}
    if(relation == "ngt") {result = if(distance <= travelDistance) true else false}
    if(relation == "nlt") {result = if(distance >= travelDistance) true else false}
    result
  }
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

object TrajectoryAvgSpeedFilter extends TrajectoryFilter{
  var averageSpeed: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.averageSpeed = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val rel = "lt"
    val avgSpeed = trajectory.getAverageSpeed
//    var result: Boolean = true
//    if(relation.equals("gt")) {result = if(avgSpeed > averageSpeed) true else false}
//    if(relation.equals("lt")) {if(avgSpeed < averageSpeed) result = true else result = false}
//    if(relation == "equal") {result = if(avgSpeed == averageSpeed) true else false}
//    if(relation == "ngt") {result = if(avgSpeed <= averageSpeed) true else false}
//    if(relation == "nlt") {result = if(avgSpeed >= averageSpeed) true else false}
//     result
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
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

object TrajectoryAvgSampleTimeFilter extends TrajectoryFilter{
  var averageSampleTime: Long = 0
  var relation: String = ""

  /**
   * Set the parameters of OTime filter
   * @param time  Start time of the trajectory
   * @param relation Relation for this parameter
   */
  def setParameters(time:Long,relation:String): Unit ={
    this.averageSampleTime = time
    this.relation = relation
  }
  def doFilter(trajectory: Trajectory): Boolean = {
    val avgInterval = trajectory.getAverageSampleInterval

    var result: Boolean = true
    if(relation == "gt") {result = if(avgInterval > averageSampleTime) true else false}
    if(relation == "lt") {result = if(avgInterval < averageSampleTime) true else false}
    if(relation == "equal") {result = if(avgInterval == averageSampleTime) true else false}
    if(relation == "ngt") {result = if(avgInterval <= averageSampleTime) true else false}
    if(relation == "nlt") {result = if(avgInterval >= averageSampleTime) true else false}
    result
  }
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
}
/**
 * Filter on a single trajectory based on whether the trajectory pass over a particular area
 */
object TrajectoryPassRangeFilter extends TrajectoryFilter{
  var range: Range =null
  def setParameters(range: Range): Unit ={
    this.range = range
  }
  override def doFilter(trajectory: Trajectory): Boolean = {
    trajectory.GPSPoints.exists(point => range.contains(point.getPoint()))
  }
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