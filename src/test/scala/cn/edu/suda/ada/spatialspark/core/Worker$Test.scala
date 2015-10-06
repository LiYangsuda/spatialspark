package cn.edu.suda.ada.spatialspark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by liyang on 15-10-6.
 */
class Worker$Test extends FunSuite {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WorkerTest"))
  Worker.setSparkContext(sc)
  test("testLoadTrajectoryFromDataSource") {
    val inputPath = "file:///home/liyang/Resources/xaa"
    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)
    System.out.println(rdd.count())
    var rdd1 = rdd
    rdd1 = rdd1.sample(false,0.5)
    println(rdd1.count)
    println(rdd.count)
  }


  test("testOriginalRDD") {

  }

  test("testToJson") {

  }

  test("testCalculateFeatures") {

  }

  test("testApplyFilters") {

  }

}
