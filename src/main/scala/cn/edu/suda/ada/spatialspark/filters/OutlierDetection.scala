package cn.edu.suda.ada.spatialspark.filters

import cn.edu.suda.ada.spatialspark.core.{Trajectory, GPSPoint}

/**
 * Created by liyang on 15-11-16.
 */
object OutlierDetection {
  /**
   * Delete all outlier points in a trajectory. If the travel speed between a GPS point and its successor is beyond the upper bound,
   * then we will delete this gps point.
   * @param GPSPoints
   * @return
   */
  def deleteOutlierPoints(GPSPoints: List[GPSPoint],speedUpperBound:Float):List[GPSPoint] = {
    var res: List[GPSPoint] = Nil
    res = GPSPoints(0)::res
    for(i <- 1 to GPSPoints.length - 1){
      val dis = GPSPoints(i).getDistance(GPSPoints(i-1))*1000
      val interval = GPSPoints(i).timestamp - GPSPoints(i-1).timestamp
      if(dis/interval < speedUpperBound)  res = GPSPoints(i)::res
    }
    res.reverse
  }

  /**
   * Delete outlier gps points in a trajectory. We use a simple method, that is when the travel speed between a GPS point and its successor beyonds the
   * given upper bound, say 35(m/s), which is the limited speed 120km/h on the highway. Since the instantaneous velocity can be higher than 120km/h,
   * the upper bound should be greater than that number. From the observation, the greatest number that make sense is 54 m/s.
   * @param traj  Trajectory
   * @param speedUpperBound  Speed upper bound
   * @return  Passed in trajectory with all outlier gps points deleted
   */
  def deleteOutlierPoints(traj:Trajectory,speedUpperBound:Float):Trajectory = {
    var res: List[GPSPoint] = Nil
    val GPSPoints = traj.GPSPoints
    res = GPSPoints(0)::res
    for(i <- 1 to GPSPoints.length - 1){
      val dis = GPSPoints(i).getDistance(GPSPoints(i-1))*1000
      val interval = GPSPoints(i).timestamp - GPSPoints(i-1).timestamp
      if(dis/interval < speedUpperBound)  res = GPSPoints(i)::res
    }
    traj.GPSPoints = res.reverse
    traj
  }
}
