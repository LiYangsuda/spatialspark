package cn.edu.suda.ada.spatialspark.core

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * A singleton object for loading trajectories from text file in HDFS. It creates RDDs based on the passed parameter path.
 * Note   Parameter path can be a directory or a text file URI in the format of "hdfs://………"
 */
trait TrajectoryLoader{
  def loadTrajectoryFromDataSource(path:String):RDD[Trajectory]
}
class HDFSTrajectoryLoader(@transient val sc: SparkContext) extends Logging with TrajectoryLoader with Serializable{
  // private  val ac = sc.accumulator(0,"trajectoryID")
    /**
     * Transform the line of String in RDD into a trajectory object. The format of the parameter string is available in {URL OF GAO TONG DATA}
     * @todo outlier detection need to be done after the trajectories have been loaded!
     * @param s A String record of the Trajectory.
     * @return  A Trajectory object.
     */

    def mapLine2Trajectory(s: String):Trajectory = {
      val fields = s.split(",")
      val trajectoryID = fields(0)

      val carID = fields(2)
      //    println(trajectoryID+","+carID)
      var GPSPoints:List[GPSPoint] = Nil

      val records:Array[String] = fields(28).split("\\|")
      //    println(records.head.split(":")(3)+"::"+records.last.split(":")(3))

      //The first gps point is different from the rest one. Deal with specially
      val firstRecord = records(0).split(":")
      val firstGPSPoint = new GPSPoint(
        firstRecord(0).toFloat/100000,      //Longitude
        firstRecord(1).toFloat/100000,      //Latitude
        firstRecord(2).toFloat,             //Speed
        firstRecord(3).toLong,              //TimeStamp
        firstRecord(4).toShort              //Direction
      )

      if(firstGPSPoint.speed < 0){firstGPSPoint.speed = 0}
      GPSPoints = firstGPSPoint :: GPSPoints
      val startTime = firstGPSPoint.timestamp
      for (record <- records.tail) {
        val recordArray = record.split(":")
        val samplePoint = GPSPoint(
          recordArray(0).toFloat / 100000 + firstGPSPoint.longitude,
          recordArray(1).toFloat / 100000 + firstGPSPoint.latitude,
          Math.round((recordArray(2).toFloat / 3.6) * 100) / 100,
          recordArray(3).toLong + startTime ,
          recordArray(4).toShort
        )
        if (samplePoint.speed < 0) {
          samplePoint.speed = 0
        }

        GPSPoints = samplePoint :: GPSPoints
      }

      val trajectory = new Trajectory(trajectoryID,carID, GPSPoints.reverse)
      if(trajectory.getDuration == 0)println(trajectoryID)
      trajectory.travelDistance = fields(14).toFloat
      trajectory
    }

  /**
   * Load trajectories from data sources
   * @param path Hadoop input path. start with "hdfs://"
   * @return Trajectory RDD of from the file
   */
   override def loadTrajectoryFromDataSource(path: String): RDD[Trajectory] = {
    logInfo("Loading trajectory data from %s".format(path))
    val lines = sc.textFile(path)
    val rdd = lines.map(line => mapLine2Trajectory(line))
    rdd
  }
}
/**
 * Factory object
 * */
object TrajectoryLoader{
  def apply(sc: SparkContext):HDFSTrajectoryLoader = {
    new HDFSTrajectoryLoader(sc)
  }
}