package cn.edu.suda.ada.spatialspark.filters

import cn.edu.suda.ada.spatialspark.core.{Range, Trajectory}

/**
 * Created by liyang on 15-9-4.
 */
trait TrajectoryFilter{
  def doFilter(trajectory:Trajectory):Boolean
  override def toString: String = "TrajectoryFilter"
}

/**
 * Filter on a single trajectory based on OTime
 */

class TrajectoryOTimeFilter(var otime: Long,var relation: String) extends TrajectoryFilter with Serializable{
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
      case _ => throw new IllegalArgumentException("relation: " +relation)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryOTimeFilter: otime = "+otime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on DTime
 */

class TrajectoryDTimeFilter(var dtime: Long,var relation: String) extends TrajectoryFilter with Serializable{

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
    val dtime_ = this.dtime
    val relation_ = this.relation
    val result: Boolean = relation_ match {
      case "gt" => t > dtime_
      case "lt" => t < dtime_
      case "equal" => t == dtime_
      case "ngt" => t <= dtime_
      case "nlt" => t >= dtime_
      case _ => throw new IllegalArgumentException(this+"relation: " +relation_)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryDTimeFilter: dtime = "+dtime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory
 */

class TrajectoryTravelTimeFilter(var traveltime: Long,var relation: String) extends TrajectoryFilter with Serializable{
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
    val traveltime_ = this.traveltime
    val relation_ = this.relation
    val result: Boolean = relation_ match {
      case "gt" => ttime > traveltime_
      case "lt" => ttime < traveltime_
      case "equal" => ttime == traveltime_
      case "ngt" => ttime <= traveltime_
      case "nlt" => ttime >= traveltime_
      case _ => throw new IllegalArgumentException("relation: " +relation)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryTravelTimeFilter: traveltime = "+traveltime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory travel distance
 */
//}
class TrajectoryTravelDistanceFilter(var travelDistance: Float,var relation: String) extends TrajectoryFilter with Serializable{

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
    val travelDistince_ = this.travelDistance
    val relation_ = this.relation
    val result: Boolean = relation_ match {
      case "gt" => distance > travelDistince_
      case "lt" => distance < travelDistince_
      case "equal" => distance == travelDistince_
      case "ngt" => distance <= travelDistince_
      case "nlt" => distance >= travelDistince_
      case _ => throw new IllegalArgumentException(this+"relation: " +relation_)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryTravelDistanceFilter: travelDistance = "+travelDistance + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

class TrajectoryAvgSpeedFilter(var averageSpeed: Float,var relation: String) extends TrajectoryFilter with Serializable{


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
    val averageSpeed_ = this.averageSpeed
    val relation_ = this.relation
//Use pattern matching to simplify the code above
    val result: Boolean = relation_ match {
      case "gt" => avgSpeed > averageSpeed_
      case "lt" => avgSpeed < averageSpeed_
      case "equal" => avgSpeed == averageSpeed_
      case "ngt" => avgSpeed <= averageSpeed_
      case "nlt" => avgSpeed >= averageSpeed_
      case _ => throw new IllegalArgumentException(this+"relation in" +this + "is"+relation_)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryAvgSpeedFilter: averageSpeed = "+averageSpeed + "relation = " + relation
}
/**
 * Filter on a single trajectory based on trajectory's average travel speed
 */

class TrajectoryAvgSampleTimeFilter( var averageSampleTime: Float,var relation: String) extends TrajectoryFilter with Serializable{

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
    val averageSampleTime_ = this.averageSampleTime
    val relation_ = this.relation
    val result: Boolean = relation_ match {
      case "gt" => avgInterval > averageSampleTime_
      case "lt" => avgInterval < averageSampleTime_
      case "equal" => avgInterval == averageSampleTime_
      case "ngt" => avgInterval <= averageSampleTime_
      case "nlt" => avgInterval >= averageSampleTime_
      case _ => throw new IllegalArgumentException(this+"avgsampleTime: "+this.averageSampleTime+" relation: "+this.relation)
    } //If all this relation don't match, an MatcherError will be thrown.
    result
  }
  override def toString = "Object TrajectoryAvgSampleTimeFilter: otime = "+averageSampleTime + "relation = " + relation
}
/**
 * Filter on a single trajectory based on range of the start point
 */

object TrajectoryOPointFilter extends TrajectoryFilter with Serializable{
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
object TrajectoryDPointFilter extends TrajectoryFilter with Serializable{
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
object TrajectoryPassRangeFilter extends TrajectoryFilter with Serializable{
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

