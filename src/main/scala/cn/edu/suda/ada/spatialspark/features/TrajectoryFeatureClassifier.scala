package cn.edu.suda.ada.spatialspark.features

import cn.edu.suda.ada.spatialspark.core.Trajectory
import org.apache.spark.Logging

/**
 * Created by liyang on 15-9-17.
 */
/**
 *  Singleton object for dividing the trajectories according to the average speed
 */
object TrajectoryAverageSpeedClassifier extends TrajectoryNumericalClassifier with Logging{
  //  var levelStep: Int = 1
  //  def setLevelStep(levelStep : Int): Unit ={
  //    this.levelStep = levelStep
  //  }
  /**
   * This method  calculates trajectory level according to its average speed and divide it into some slot.
   * Note that we map this trajectory to its low bound speed slot. For example, if the levelstep is 2 and the trajectory A's
   * average speed is 1.8m/s, we map it to level 0
   * @param trajectory The trajectory for calculating level.
   * @return The level of this trajectory
   */

  def getLevel(trajectory:Trajectory,levelStep: Int):Int = {
    val avgSpeed = trajectory.getAverageSpeed
    val level =  (avgSpeed / levelStep).toInt * levelStep
    level
  }
}

/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryTravelDistanceClassifier extends TrajectoryNumericalClassifier{
  var levelStep: Int = 1
  def setLevelStep(levelStep : Int): Unit ={
    this.levelStep = levelStep
  }
  def getLevel(trajectory:Trajectory,levelStep: Int):Int = {
    val travelDistance = trajectory.getTravelDistance
    val level =  (travelDistance / levelStep).toInt * levelStep
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryTravelTimeClassifier extends TrajectoryNumericalClassifier{
  var levelStep: Int = 1
  def setLevelStep(levelStep : Int): Unit ={
    this.levelStep = levelStep
  }
  def getLevel(trajectory:Trajectory,levelStep: Int):Int = {
    val travelTime = trajectory.getDuration

    val level =  (travelTime / levelStep).toInt * levelStep
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryAvgSimpleTimeClassifier extends TrajectoryNumericalClassifier{
  var levelStep: Int = 1
  def setLevelStep(levelStep : Int): Unit ={
    this.levelStep = levelStep
  }
  def getLevel(trajectory:Trajectory,levelStep:Int):Int = {
    val sampleInterval = trajectory.getAverageSampleInterval
    val level = (sampleInterval / levelStep).toInt* levelStep
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectorySimplePointsCountClassifier extends TrajectoryNumericalClassifier{
  var levelStep: Int = 1
  def setLevelStep(levelStep : Int): Unit ={
    this.levelStep = levelStep
  }
  def getLevel(trajectory:Trajectory,levelStep: Int):Int = {
    val samplePoints = trajectory.length
    val level =  (samplePoints / levelStep) * levelStep
    level
  }
}

object GPSSamplePointSpeedClassifier extends GPSPointSampleSpeedClassifier{

  def getDistribution(trajectory: Trajectory,levelStep: Int): Seq[(Int, Int)] = {
    val pointMap: Seq[(Int,Int)] =  trajectory.GPSPoints.map(p =>(1, getLevel(p,levelStep)))
    pointMap
  }

}