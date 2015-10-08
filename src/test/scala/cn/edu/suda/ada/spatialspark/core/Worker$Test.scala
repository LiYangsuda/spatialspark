package cn.edu.suda.ada.spatialspark.core

import cn.edu.suda.ada.spatialspark.features.{TrajectoryTravelDistanceClassifier, TrajectoryAverageSpeedClassifier}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec

/**
 * Created by liyang on 15-10-6.
 */
class WorkerTest extends FlatSpec {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WorkerTest"))
  Worker.setSparkContext(sc)
  var rdd: RDD[Trajectory] = null
  val inputPath = "hdfs://192.168.131.192:9000/data/xaa"
  rdd = Worker.loadTrajectoryFromDataSource(inputPath)
  System.out.println(rdd.count())

  "There " should "be 100 trajectories " in{
    rdd.foreach(tra => println(tra.getRectangle))
  }

  "After applyFilters(PassRange), the trajectory number " should "be less than 100" in {
    println("Before: "+rdd.count())
    val filterMap:Map[String,Map[String,String]] = Map("PassRange" -> Map("minLat" -> "33.226254", "maxLat" -> "46.614256","minLng" -> "107.676595", "maxLng" -> "125.130049"))
    val rdd2 = Worker.applyFilters(filterMap)
    println("After: "+rdd2.count())
  }

}
