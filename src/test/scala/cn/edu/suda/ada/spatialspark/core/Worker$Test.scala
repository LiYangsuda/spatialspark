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
  rdd.foreach(tra => {
    tra.GPSPoints.foreach(p => {if(p.speed<0) println("Illegal speed"+p.speed)})
  })
  System.out.println(rdd.count())
//  val num = rdd.map(tra => tra.GPSPoints.length).sum()
//  println(num)
//  "There " should "be 100 trajectories " in{
//    rdd.foreach(tra => println(tra.getRectangle))
//  }
//
//  "After applyFilters(PassRange), the trajectory number " should "be less than 100" in {
//    println("Before: "+rdd.count())
//    val filterMap:Map[String,Map[String,String]] = Map("PassRange" -> Map("minLat" -> "33.226254", "maxLat" -> "46.614256","minLng" -> "107.676595", "maxLng" -> "125.130049"))
//    val rdd2 = Worker.applyFilters(filterMap)
//    println("After: "+rdd2.count())
//  }

//  "feature distribution " should "be correct before apply method toJson" in{
//    rdd.foreach(tra =>  println(tra.getDuration))
//    val filterPara = Map("TravelDistance"-> Map("value" -> "5000", "relation" -> "lt"))
//    val rdd2 = Worker.applyFilters(filterPara)
//    val featurePara = Map("TrajTravelTime"->3600)
//    val feature = Worker.calculateFeatures(featurePara)
//    feature.foreach(f => {
//      f._2.foreach(ff =>{
//        println(ff._1 +"->"+ff._2)
//      })
//    })
//    println(Worker.toJson(feature,featurePara))
//  }
  "No negative data " should "be in the result" in{
    val feature = Map("GPSSampleSpeed"->10)
    val distribution = Worker.calculateFeatures(feature)
    featureDisplay(distribution)

  }
  def featureDisplay(distributions :Map[String, Array[(Int, Int)]]) = {
    distributions.foreach(f => {
      f._2.foreach(ff =>{
        println(ff._1 +"->"+ff._2)
      })
    })
  }
}
