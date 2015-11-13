package cn.edu.suda.ada.spatialspark.filters

import cn.edu.suda.ada.spatialspark.core.{Range, Trajectory}

/**
 * Created by liyang on 15-10-12.
 */

trait GPSPointFilter extends Serializable{

  def doFilter(trajectory: Trajectory): Trajectory
  override def toString = "GPSPointFilter"
}
/**
 * Filter on GPSPoints based on whether the points are in the range of the passed in range
 * This will get a sub trajectory of the original one.
 */
object GPSPointRangeFilter extends GPSPointFilter{
  var range : Range = _
  def setParameters(range: Range): Unit ={
    this.range = range
  }
  /**
   * Get a sub trajectory of the original one, whose GPS Points are all int the range
   * @param trajectory
   * @return
   */
  override def doFilter(trajectory: Trajectory): Trajectory = {
    if(range == null) throw new NullPointerException("range is null in %s"+this.getClass.getSimpleName)
    trajectory.getSubTrajectory(range)
  }

  override def toString = "GPSPointRangeFilter: range = " + range.toString
}

/**
 * Filter on GPSPoints based on whether the points' speeds are in the range
 * This will get a sub trajectory of the original one.
 */
object GPSPointSampleSpeedFilter extends GPSPointFilter{
  var interval: Int = _
  val relation: String = ""

  //def setParameters()
  /**
   * Get a sub trajectory of the original one, whose GPS Points' speeds are all int the range
   * @param trajectory
   * @return
   */
  override def doFilter(trajectory: Trajectory): Trajectory = {
    if(interval == 0) throw new NullPointerException("range is null in %s"+this.getClass.getSimpleName)
    trajectory.getSubTrajectory(interval)
  }

  override def toString = "GPSPointRangeFilter: range = " + interval
}
