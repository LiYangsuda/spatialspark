package cn.edu.suda.ada.spatialspark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by liyang on 15-10-6.
 */
class Worker$Test extends FunSuite {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WorkerTest"))
  Worker.setSparkContext(sc)
  var rdd: RDD[Trajectory] = null
  test("testLoadTrajectoryFromDataSource") {
    val inputPath = "hdfs://192.168.131.192:9000/data/xaa"
    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)
    System.out.println(rdd.count())
    val filterMap:Map[String,Map[String,String]] = Map("AvgSpeed" -> Map("value" ->"20","relation" -> "gt"))
    val rdd2 = Worker.applyFilters(filterMap)
    println("after:"+rdd2.count())
  }
 // Map[String,Map[String,String]]
//  test("test rdd copy") {
//    var rdd1 = rdd
//    rdd1 = rdd1.sample(false,0.5)
//    println(rdd1.count)
//    println(rdd.count)
//  }

}
