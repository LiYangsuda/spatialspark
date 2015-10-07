package cn.edu.suda.ada.spatialspark.core

import cn.edu.suda.ada.spatialspark.features.{TrajectoryTravelDistanceClassifier, TrajectoryAverageSpeedClassifier}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec

/**
 * Created by liyang on 15-10-6.
 */
class Worker$Test extends FlatSpec {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WorkerTest"))
  Worker.setSparkContext(sc)
  var rdd: RDD[Trajectory] = null
  "A number" should "be less than 100" in{
    val inputPath = "hdfs://192.168.131.192:9000/data/xaa"
    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)
    System.out.println(rdd.count())
    val filterMap:Map[String,Map[String,String]] = Map("TravelDistance" -> Map("value" -> "1000","relation"->"gt"))
    val rdd2 = Worker.applyFilters(filterMap)
    println("after:"+rdd2.count())
    val dis = Map("TrajTravelDistance"->1000)
    TrajectoryTravelDistanceClassifier.setLevelStep(100)
    val feature = Worker.getFeatures(TrajectoryTravelDistanceClassifier.getLevel)
    feature.foreach(println _)

  }
}
