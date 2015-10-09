package cn.edu.suda.ada.spatialspark.features

import cn.edu.suda.ada.spatialspark.core.{GPSPoint, Trajectory}

/**
 * Created by liyang on 15-9-4.
 */
/**
 * Divide  the trajectories categories according to passed parameter levelstep
 * The only reason why NumericalClassifier exist is the method getLevel. getLevel calculate the level of the trajectory
 * and return the category it belongs to. Subclass the this class divider trajectories according to different features
 */

trait TrajectoryNumericalClassifier{
  var levelStep: Int = 1
  def setLevelStep(levelStep : Int): Unit ={
    this.levelStep = levelStep
  }
  /**
   * For a given trajectory, calculate which feature level it belongs to.
   * @param trajectory The trajectory for calculating level.
   * @return The level of this trajectory
   */
  def getLevel(trajectory:Trajectory):Int
}

/**
 * For a given trajectory, loop through all the sample points and calculate the level for each point.
 * The method getDistribution return the
 */
trait  GPSPointNumericalClassifier extends Serializable{
  var levelStep : Int = 1
  def setLevelStep(level: Int): Unit ={
    this.levelStep = level
  }
  def getLevel(point: GPSPoint):Int = {
    throw new NoClassDefFoundError("Please create a subclass of GPSPoingNumericalClassifier")
  }
}
trait GPSPointSampleSpeedClassifier extends GPSPointNumericalClassifier{
  override def getLevel(point: GPSPoint):Int = {
    (point.speed / levelStep).toInt
  }
}
