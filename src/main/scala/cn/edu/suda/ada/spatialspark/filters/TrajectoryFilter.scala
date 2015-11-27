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

private class TrajectoryOTimeFilter(var otime: Long,var relation: String) extends TrajectoryFilter with Serializable{
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
      case "equal" => t >= (otime - 60) &&  t <=  (otime + 60)
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

 private class TrajectoryDTimeFilter(var dtime: Long,var relation: String) extends TrajectoryFilter with Serializable{

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
      case "equal" => t >= (dtime_ - 60) &&  t <=  (dtime_ + 60)
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

private class TrajectoryTravelTimeFilter(var traveltime: Long,var relation: String) extends TrajectoryFilter with Serializable{
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
private class TrajectoryTravelDistanceFilter(var travelDistance: Float,var relation: String) extends TrajectoryFilter with Serializable{

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

private class TrajectoryAvgSpeedFilter(var averageSpeed: Float,var relation: String) extends TrajectoryFilter with Serializable{


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

private class TrajectoryAvgSampleTimeFilter( var averageSampleTime: Float,var relation: String) extends TrajectoryFilter with Serializable{

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

private class TrajectoryOPointFilter(var range: Range) extends TrajectoryFilter with Serializable{

  def setParameters(range: Range): Unit ={
    this.range = range
  }
  def doFilter(trajectory:Trajectory):Boolean = {
    val range_ = this.range
    val p = trajectory.getStartPoint
    range_.contains(p.getPoint())
  }
  override def toString = "Object TrajectoryOPointFilter: range = "+range.toString
}
/**
 * Filter on a single trajectory based on range of the end point
 */
class TrajectoryDPointFilter(var range: Range) extends TrajectoryFilter with Serializable{

  def setParameters(range:Range): Unit ={
    this.range = range
  }

  override def doFilter(trajectory: Trajectory): Boolean = {
    val p = trajectory.getEndPoint
    val range_ = this.range
    range_.contains(p.getPoint())
  }
  override def toString = "TrajectoryDPointFilter: range = "+range.toString
}
/**
 * Filter on a single trajectory based on whether the trajectory pass over a particular area
 */
 private class TrajectoryPassRangeFilter(var range: Range) extends TrajectoryFilter with Serializable{

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
    val range_ = this.range
    trajectory.GPSPoints.exists(point => range_.contains(point.getPoint()))
  }

  override def toString = "Object TrajectoryPassRangeFilter: range = "+range.toString
}
private class TrajectoryRangeFilter(var range: Range) extends TrajectoryFilter with Serializable{

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
    val range_ = this.range
    val subTrajectory = trajectory.getSubTrajectory(range_)
    if(subTrajectory.GPSPoints.length != 0) {
      trajectory.GPSPoints = subTrajectory.GPSPoints
      true
    }else{
      false
    }
  }

  override def toString = "Object TrajectoryPassRangeFilter: range = "+range.toString
}
object TrajectoryFilter{
  def apply(filtersParameters : Map[String,Map[String,String]]): List[TrajectoryFilter] ={
    var filters: List[TrajectoryFilter] = Nil
    for(filter <- filtersParameters){
      if(filter._1 == "OTime"){
        val item = new TrajectoryOTimeFilter(filter._2("value").toLong,filter._2("relation"))
        filters =  item :: filters
      }
      if(filter._1 == "DTime"){
        val item = new TrajectoryDTimeFilter(filter._2("value").toLong,filter._2("relation"))

        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("TravelTime")){
        val item = new TrajectoryTravelTimeFilter(filter._2("value").toLong,filter._2("relation"))
        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("TravelDistance")){
        val item  = new TrajectoryTravelDistanceFilter(filter._2("value").toFloat,filter._2("relation"))
        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("AvgSpeed")){
        val item = new TrajectoryAvgSpeedFilter(filter._2("value").toLong,filter._2("relation"))

        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("AvgSampleTime")){
        val item = new  TrajectoryAvgSampleTimeFilter(filter._2("value").toLong,filter._2("relation"))
        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("OPoint")){
        val range = new Range(filter._2("minLng").toDouble,filter._2("maxLat").toDouble,filter._2("maxLng").toDouble,filter._2("minLat").toDouble)
        val item = new  TrajectoryOPointFilter(range)
        filters =   item :: filters
      }
      if(filter._1.equalsIgnoreCase("DPoint")){
        val range = new Range(filter._2("minLng").toDouble,filter._2("maxLat").toDouble,filter._2("maxLng").toDouble,filter._2("minLat").toDouble)
        val item = new  TrajectoryDPointFilter(range)
        filters =    item :: filters
      }
      if(filter._1.equalsIgnoreCase("PassRange")){
        val range = new Range(filter._2("minLng").toDouble,filter._2("maxLat").toDouble,filter._2("maxLng").toDouble,filter._2("minLat").toDouble)
        val item = new TrajectoryPassRangeFilter(range)
        filters =    item :: filters
      }
      if(filter._1.equalsIgnoreCase("Range")){
        val range = new Range(filter._2("minLng").toDouble,filter._2("maxLat").toDouble,filter._2("maxLng").toDouble,filter._2("minLat").toDouble)
        val item = new TrajectoryRangeFilter(range)
        filters =    item :: filters
      }
    }
    filters
  }
  def apply(filterName: String,filterValue: String,filterRelation: String): TrajectoryFilter = {

    val trajectoryFilter: TrajectoryFilter = filterName match {
      case "OTime" => new TrajectoryOTimeFilter(filterValue.toLong, filterRelation)
      case "DTime" => new TrajectoryDTimeFilter(filterValue.toLong, filterRelation)
      case "TravelTime" => new TrajectoryTravelTimeFilter(filterValue.toLong, filterRelation)
      case "TravelDistance" => new TrajectoryTravelDistanceFilter(filterValue.toFloat, filterRelation)
      case "AvgSpeed" => new TrajectoryAvgSpeedFilter(filterValue.toFloat, filterRelation)
      case "AvgSampleSpeed" => new TrajectoryAvgSampleTimeFilter(filterValue.toLong, filterRelation)
    }
    trajectoryFilter
  }
   def apply(filterName: String,minLng: Double,maxLat:Double,maxLng:Double,minLat:Double): TrajectoryFilter = {
     val range = Range(minLng,maxLat,maxLng,minLat)
    val trajectoryFilter: TrajectoryFilter = filterName match {
      case "OPoint" => new TrajectoryOPointFilter(range)
      case "DPoint" => new TrajectoryDPointFilter(range)
      case "PassRange" => new TrajectoryPassRangeFilter(range)
    }
     trajectoryFilter
   }
}