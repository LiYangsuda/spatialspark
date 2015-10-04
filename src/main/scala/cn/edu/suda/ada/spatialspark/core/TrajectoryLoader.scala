package cn.edu.suda.ada.spatialspark.core

/**
 * A singleton object for loading trajectories from text file in HDFS. It creates RDDs based on the passed parameter path.
 * Note   Parameter path can be a directory or a text file URI in the format of "hdfs://………"
 */
trait TrajectoryLoader{
  def loadTrajectoryFromDataSource(path:String)
}

object HDFSTrajectoryLoader{
  /**
   * Transform the line of String in RDD into a trajectory object. The format of the parameter string is available in {URL OF GAO TONG DATA}
   * @param s A String record of the Trajectory.
   * @return  A Trajectory object.
   */
  def mapLine2Trajectory(s: String):Trajectory = {
    val fields = s.split(",")
    val trajectoryID = fields(1)
    val carID = fields(3)
    var GPSPoints:List[GPSPoint] = Nil
    val firstGPSPoint = new GPSPoint(fields(22).toFloat,fields(23).toFloat,0,fields(26).toInt,0)
    GPSPoints = firstGPSPoint:: GPSPoints
    val records:Array[String] = fields(28).split("\\|")
    for(record <- records){
      val recordArray = record.split(":")
      val samplePoint = GPSPoint(recordArray(0).toFloat/100000+firstGPSPoint.latitude,recordArray(1).toFloat/100000+firstGPSPoint.longitude,
        recordArray(2).toFloat,recordArray(3).toLong+firstGPSPoint.timestamp,recordArray(4).toFloat)
      GPSPoints = samplePoint :: GPSPoints
    }

    val trajectory = new Trajectory(trajectoryID,carID, GPSPoints.reverse)
    trajectory.travelDistance = fields(14).toFloat
    trajectory
  }
}
