package cn.edu.suda.ada.spatialspark.features

import cn.edu.suda.ada.spatialspark.core.Trajectory

/**
 * Created by liyang on 15-9-17.
 */
/**
 *  Singleton object for dividing the trajectories according to the average speed
 */
object TrajectoryAverageSpeedClassifier extends TrajectoryNumericalClassifier{
  /**
   * This method  calculates trajectory level according to its average speed and divide it into some slot.
   * Note that we map this trajectory to its low bound speed slot. For example, if the levelstep is 2 and the trajectory A's
   * average speed is 1.8m/s, we map it to level 0
   * @param trajectory The trajectory for calculating level.
   * @return The level of this trajectory
   */

  def getLevel(trajectory:Trajectory):Int = {
    val avgSpeed = trajectory.getAverageSpeed
    val level =  (Math.floor(avgSpeed / levelStep) * levelStep).toInt
    level
  }
}

/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryTravelDistanceClassifier extends TrajectoryNumericalClassifier{

  def getLevel(trajectory:Trajectory):Int = {
    val travelDistance = trajectory.length
    val level =  (Math.floor(travelDistance / levelStep) * levelStep).toInt
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryTravelTimeClassifier extends TrajectoryNumericalClassifier{

  def getLevel(trajectory:Trajectory):Int = {
    val travelTime = trajectory.getEndTime - trajectory.getStarTime
    val level =  (Math.floor(travelTime / levelStep) * levelStep).toInt
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectoryAvgSimpleTimeClassifier extends TrajectoryNumericalClassifier{

  def getLevel(trajectory:Trajectory):Int = {
    val sampleInterval = trajectory.getAverageSampleInterval
    val level =  (Math.floor(sampleInterval / levelStep) * levelStep).toInt
    level
  }
}
/**
 * Singleton object for dividing the trajectories according to the passed travel distance level step parameter
 */
object TrajectorySimplePointsCountClassifier extends TrajectoryNumericalClassifier{

  def getLevel(trajectory:Trajectory):Int = {
    val samplePoints = trajectory.length
    val level =  (Math.floor(samplePoints / levelStep) * levelStep).toInt
    level
  }
}
