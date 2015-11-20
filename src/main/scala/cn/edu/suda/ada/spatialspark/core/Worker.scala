package cn.edu.suda.ada.spatialspark.core

import cn.edu.suda.ada.spatialspark.features._
import cn.edu.suda.ada.spatialspark.filters._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD


/**
 * Created by liyang on 15-9-22.
 */
/**
 * The worker deamon is responsible for loading data from data sources and precessing data according to the parameters from
 * user client
 */
object Worker extends Logging {
  var sc: SparkContext = _
  var rdd: RDD[Trajectory] = null
  var originalRDD: RDD[Trajectory] = null
  var points: String = _
  /**
   * Set the spark sc for the worker, which will be used to loading trajectory from file
   * @param sc Sparksc
   */
  def setSparkContext(sc: SparkContext) = {
    this.sc = sc
  }

  def setRDDReference(rdd: RDD[Trajectory]): Unit ={
    this.originalRDD = rdd
    originalRDD.persist(StorageLevel.MEMORY_ONLY)
    originalRDD.setName("Original Trajectory RDD")

    logInfo("There are %d number of trajectories".format(originalRDD.count()))
  }
//  /**
//   * Load trajectories from data sources
//   * @param path Hadoop input path. start with "hdfs://"
//   * @return Trajectory RDD of from the file
//   */
//  def loadTrajectory(path: String): RDD[Trajectory] = {
//    if (sc == null) throw new Exception("Sparksc haven't been initialized in the worker")
//    else {
//      val lines = sc.textFile(path)
//      originalRDD = lines.map(line => HDFSTrajectoryLoader.mapLine2Trajectory(line)).persist()
//      val trajectoryCount = originalRDD.count()
//      logInfo("There are" + trajectoryCount + "trajectories in the sampling data")
//
//      originalRDD
//    }
//  }
 // def loadTrajectory(path:String):RDD[Trajectory]

  /**
   * Get the feature distribution for the given feature
   *   --Note Here, we use collect to collect all result from the cluster, which will have a big impact on the performance
   * @param feature  a function value that calculate the feature level of the trajectory
   * @return  feature distribution
   * @todo the second map is unnecessary
   */
  def getTrajFeatures(feature: Trajectory => Int): Array[(Int, Int)] = {
    val appliedRDD = if(rdd == null) originalRDD else rdd
    val MapRDD = appliedRDD.map(trajectory => (feature(trajectory),1)).reduceByKey(_ + _, 4).sortByKey(true).collect()
    MapRDD
  }
  def getGPSFeatures(feature: Trajectory => Seq[(Int,Int)]):Array[(Int,Int)] = {
    val appliedRDD = if(rdd == null) originalRDD else rdd
    val mapRdd = appliedRDD.flatMap(trajectory => feature(trajectory)).map(t => (t._2,t._1)).reduceByKey(_ + _,4).sortByKey(true).collect()
    mapRdd
  }
  /**
   * For the feature parameters in the http request, calculate each feature distribution and save the distribution in the result type Array[(Int,Int)]
   * Finally, wrap all feature distribution in the format Map("FEATURE_NAME" -> DISTRIBUTION_OF_THAT_FEATURE)
   * ＠param trajectoryRDD trajectory data　
   * @param features all features need to calculated. the parameter type is Map[String,Int], the key indicate the feature name and the value is the level step of that feature
   * @return
   */
  def calculateFeatures(features: Map[String, Int]): Map[String, Array[(Int, Int)]] = {
    // def calculateFeatures(features: Map[String,Int]):Worker = {
    logInfo("calculating the features:")

    var distributions = Map[String, Array[(Int, Int)]]()
    for ((featureName,levelStep) <- features) {
      val distribution =
        featureName match {

          case "TrajAvgSpeed" => {
            getTrajFeatures((tra: Trajectory) => TrajectoryAverageSpeedClassifier.getLevel(tra,levelStep))
          }
          case "TrajTravelDistance" => {
            getTrajFeatures((tra: Trajectory) => TrajectoryTravelDistanceClassifier.getLevel(tra,levelStep))
          }
          case "TrajTravelTime" => {
            getTrajFeatures((tra: Trajectory) => TrajectoryTravelTimeClassifier.getLevel(tra,levelStep))
          }
          case "TrajSamplePointsCount" => {
            getTrajFeatures((tra: Trajectory) => TrajectorySimplePointsCountClassifier.getLevel(tra,levelStep))
          }
          case "TrajAvgSampleTime" => {
            getTrajFeatures((tra: Trajectory) => TrajectoryAvgSimpleTimeClassifier.getLevel(tra,levelStep))
          }
          case "GPSSampleSpeed" => {
            getGPSFeatures((tra: Trajectory) => GPSSamplePointSpeedClassifier.getDistribution(tra,levelStep))
          }
        }
      distributions += (featureName -> distribution)
    }
    points = takeSamplePoints()
    if(rdd != null) rdd.unpersist()
    rdd = null
    distributions
  }

  /**
   * data: [{lnglat: [116.405285, 39.904989], name: i,id:1},{}, …]
   * @return
   */
  private def takeSamplePoints():String ={
    println("take sample points")
    var samplePoints = ""
    var trajectories: Array[Trajectory] = null
    if(rdd != null){trajectories = rdd.takeSample(true,200)}
    else {trajectories = originalRDD.takeSample(true,200)}
 //   trajectories.foreach(traj => points += trajectoryPoints(traj))
    samplePoints = "[" + trajectories.map(traj => trajectoryPoints(traj)).mkString(",")+"]"
    samplePoints
  }
  private  def trajectoryPoints(traj: Trajectory): String = {
    val gpsPoints = traj.GPSPoints
    val str = gpsPoints.map(p => "{\"lnglat\":["+p.longitude+","+p.latitude+"]}").mkString(",")
    str
  }
  /**
   * Transform all features' distribution data into json format.
   * @param distributions  Type: Map[featureName:String,featureDistribution: Array[(Int,Int)]   All feature distributions.
   * @param featureLevelSteps All feature level step, Type:Map[featureName:String,levelStep:Int]
   * @return Distribution data in JSON format
   */
  def toJson(distributions: Map[String, Array[(Int, Int)]], featureLevelSteps: Map[String, Int]): String = {
    val jsonSet = for (singleDistribution <- distributions) yield {
      "\"" + singleDistribution._1 + "\":" + array2JsonArray(singleDistribution._2, featureLevelSteps(singleDistribution._1))
    }
    val jsonData = "{" + jsonSet.mkString(",") + ",\"points\":"+points+"}"
    jsonData
  }

  /**
   * Transform the feature distribution into json array
   * @param distribution Distribution of one particular feature
   * @param levelStep  The level step that users want to segment the distribution data
   * @return   distribution data in the format of json array without the prefix of the  feature's name, like [{"down": 0,"up": 2,"val": 111},{},……]
   */
  private def array2JsonArray(distribution: Array[(Int, Int)], levelStep: Int = 1): String = {
    var jsonMap: List[String] = Nil
    /*Transform each feature distribution to json key-value pair. Take trajectory average speed for example,
     if the level step is 2,the distribution data will be transformed into
     [{"down":0,"up":2,"val":1111},{"down":2,"up":4,"val":2222},……]
    */
    for (item <- distribution) {
      //item is a two element tuple,for example,(0,2222)
      val jsonItem = "{\"down\":" + item._1 + ",\"up\":" + (item._1 + levelStep) + ",\"val\":" + item._2 + "}"
      jsonMap =  jsonItem :: jsonMap
    }
    "[" + jsonMap.reverse.mkString(",") + "]"
  }

  /**
   * Construct a filter list given the http request parameters
   * @param filtersParameters
   * @return
   */
  private  def constructFilters(filtersParameters: Map[String,Map[String,String]]):List[TrajectoryFilter] = {
    var filters: List[TrajectoryFilter] = Nil
    filters = TrajectoryFilter(filtersParameters)
    filters
  }

  /**
   * Apply filters on the trajectories. All the filter parameters are come from end users through http requests
   * @param filtersParameters  filter parameters from the request
   * @return rdd after applying filters
   * @todo NOTE: Here I want to use a broadcast variable to reduce the cost of constructing filter lists every time but failed.
   */
  def applyFilters(filtersParameters: Map[String,Map[String,String]]):RDD[Trajectory] = {
    val filtersBroadcast = sc.broadcast(constructFilters(filtersParameters))

    if (originalRDD == null) throw new Exception("Error: trajectory data is null in Worker.applyFilters()")
    logInfo("Applying filters on trajectory rdd")

    rdd = originalRDD.filter(tra => {
      var flag = true
      //val filters = Worker.constructFilters(filtersParameters)
      val filters = filtersBroadcast.value
      for(filter <- filters if flag) {
        flag = flag && filter.doFilter(tra)
      }
      flag
    }).setName("Temp RDD").cache()
//    if(filtersParameters.contains("Range")) {
//      val params = filtersParameters.get("Range")
//      val range = new Range(params.get("minLng").toDouble, params.get("maxLat").toDouble, params.get("maxLng").toDouble, params.get("minLat").toDouble)
//      val gpsFilter = new GPSPointRangeFilter(range)
//      val bc = sc.broadcast(gpsFilter)
//
//      val rdd2 = rdd.filter(traj =>{
//        val filter = bc.value
//        filter.doFilter(traj)})
//      //rdd2.foreach(t => println(t.GPSPoints.length))
//      rdd = rdd2
//    }

    logInfo(rdd.partitions.length.toString)
    rdd
  }
}
