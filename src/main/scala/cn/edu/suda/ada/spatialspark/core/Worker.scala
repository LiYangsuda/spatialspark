package cn.edu.suda.ada.spatialspark.core

import cn.edu.suda.ada.spatialspark.features._
import cn.edu.suda.ada.spatialspark.filters._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by liyang on 15-9-22.
 */
/**
 * The worker deamon is responsible for loading data from data sources and precessing data according to the parameters from
 * user client
 */
object Worker extends Logging {
  var context: SparkContext = null
  var rdd: RDD[Trajectory] = null
  var originalRDD: RDD[Trajectory] = null

  /**
   * Set the spark context for the worker, which will be used to loading trajectory from file
   * @param sc SparkContext
   */
  def setSparkContext(sc: SparkContext) = {
    context = sc
  }

  /**
   * Load trajectories from data sources
   * @param path Hadoop input path. start with "hdfs://"
   * @return Trajectory RDD of from the file
   */
  def loadTrajectoryFromDataSource(path: String): RDD[Trajectory] = {
    if (context == null) throw new Exception("SparkContext haven't been initialized in the worker")
    else {
      val lines = context.textFile(path)
      originalRDD = lines.map(HDFSTrajectoryLoader.mapLine2Trajectory).persist()
      val trajectoryCount = originalRDD.count()
      logInfo("There are" + trajectoryCount + "trajectories in the sampling data")

      originalRDD
    }
  }

  /**
   * Get the feature distribution for the given feature
   * @param feature  a function value that calculate the feature level of the trajectory

   * @return  feature distribution
   */
   def getFeatures(feature: Trajectory => Int): Array[(Int, Int)] = {

    val MapRDD = rdd.map(trajectory => (1, feature(trajectory))).map(t => (t._2, t._1)).reduceByKey(_ + _, 1).sortByKey(true).collect()
    MapRDD
  }

  /**
   * For the feature parameters in the http request, calculate each feature distribution and save the distribution in the result type Array[(Int,Int)]
   * Finally, wrap all feature distribution in the format Map("FEATURE_NAME" -> DISTRIBUTION_OF_THAT_FEATURE)
   * ＠param trajectoryRDD trajectory data　
   * @param features all features need to calculated. the parameter type is Map[String,Int], the key indicate the feature name and the value is the level step of that feature
   * @return
   */
  def calculateFeatures(features: Map[String, Int]): Map[String, Array[(Int, Int)]] = {
    if (rdd == null) rdd = originalRDD
    logInfo("calculating the features")
    var distributions = Map[String, Array[(Int, Int)]]()
    for ((featureName,levelStep) <- features) {
      val distribution =
        featureName match {
          case "TrajAvgSpeed" => {
            TrajectoryAverageSpeedClassifier.setLevelStep(levelStep)
            getFeatures(TrajectoryAverageSpeedClassifier.getLevel)
          }
          case "TrajTravelDistance" => {
            TrajectoryTravelDistanceClassifier.setLevelStep(levelStep)
            getFeatures(TrajectoryTravelDistanceClassifier.getLevel)
          }
          case "TrajTravelTime" => {
            TrajectoryTravelDistanceClassifier.setLevelStep(levelStep)
            getFeatures(TrajectoryTravelTimeClassifier.getLevel)
          }
          case "TrajSimplePointsCount" => {
            TrajectorySimplePointsCountClassifier.setLevelStep(levelStep)
            getFeatures(TrajectorySimplePointsCountClassifier.getLevel)
          }
          case "TrajAvgSimpleTime" => {
            TrajectoryAvgSimpleTimeClassifier.setLevelStep(levelStep)
            getFeatures(TrajectoryAvgSimpleTimeClassifier.getLevel)
          }
        }
      distributions += (featureName -> distribution)
    }
    distributions
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
    val jsonData = "{" + jsonSet.mkString(",") + "}"
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


  private  def constructFilters(filtersParameters: Map[String,Map[String,String]]):List[TrajectoryFilter] = {
    var filters: List[TrajectoryFilter] = Nil
    for(filter <- filtersParameters){
      if(filter._1 == "OTime"){
        TrajectoryOTimeFilter.setParameters(filter._2("value").toLong,filter._2("relation"))
        logInfo("Applying OTime filter on rdd. OTime.value = "+filter._2("value")+" filter relation: "+filter._2("relation"))
       filters =  TrajectoryOTimeFilter :: filters
      }
      if(filter._1 == "DTime"){
        TrajectoryDTimeFilter.setParameters(filter._2("value").toLong,filter._2("relation"))
        logInfo("Applying DTime filter on rdd")
        filters =   TrajectoryDTimeFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("TravelTime")){
        TrajectoryTravelTimeFilter.setParameters(filter._2("value").toLong,filter._2("relation"))
        logInfo("Applying TravelTime filter on rdd")
        filters =   TrajectoryTravelTimeFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("TravelDistance")){
        TrajectoryTravelDistanceFilter.setParameters(filter._2("value").toDouble,filter._2("relation"))
        logInfo("Applying TravelDistance filter on rdd")
        filters =   TrajectoryTravelDistanceFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("AvgSpeed")){
        TrajectoryAvgSpeedFilter.setParameters(filter._2("value").toLong,filter._2("relation"))
        logInfo("before apply filter:AvgSpeed:= "+filter._2("value")+filter._2("relation"))
        filters =   TrajectoryAvgSpeedFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("AvgSampleTime")){
        TrajectoryAvgSampleTimeFilter.setParameters(filter._2("value").toLong,filter._2("relation"))
        logInfo("Applying AvgSampleTime filter on rdd")
        filters =   TrajectoryAvgSampleTimeFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("OPoint")){
        val range = new Range(filter._2("minLat").toDouble,filter._2("maxLat").toDouble,filter._2("minLng").toDouble,filter._2("maxLng").toDouble)
        logInfo("Applying OPoint filter on rdd")
        TrajectoryOPointFilter.setParameters(range)
        filters =   TrajectoryOPointFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("DPoint")){
        val range = new Range(filter._2("minLat").toDouble,filter._2("maxLat").toDouble,filter._2("minLng").toDouble,filter._2("maxLng").toDouble)
        logInfo("Applying DPoint filter on rdd")
        TrajectoryDPointFilter.setParameters(range)
        filters =    TrajectoryDPointFilter :: filters
      }
      if(filter._1.equalsIgnoreCase("PassRange")){
        val range = new Range(filter._2("minLat").toDouble,filter._2("maxLat").toDouble,filter._2("minLng").toDouble,filter._2("maxLng").toDouble)
        logInfo("Applying PassRange filter on rdd")
        TrajectoryPassRangeFilter.setParameters(range)
        filters =    TrajectoryPassRangeFilter :: filters
      }
    }
    filters
  }

  def applyFilters(filtersParameters: Map[String,Map[String,String]]):RDD[Trajectory] = {
    val filters: List[TrajectoryFilter] = constructFilters(filtersParameters)

    logInfo("Filter number: "+filters.length)
    if (originalRDD == null) throw new Exception("Error: trajectory data is null in Worker.applyFilters()")
    if(rdd == null)   rdd = originalRDD
    logInfo("Applying filters on trajectory rdd")
    val filterNums = filters.length
    rdd = rdd.filter(tra => {
      var flag = true
      for(filter <- filters if flag) flag = flag && filter.doFilter(tra)
      flag
    })
    rdd
  }
}
