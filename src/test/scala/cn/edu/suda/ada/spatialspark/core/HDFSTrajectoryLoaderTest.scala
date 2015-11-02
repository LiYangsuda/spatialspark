package cn.edu.suda.ada.spatialspark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by liyang on 15-10-31.
 */
class HDFSTrajectoryLoaderTest extends FunSuite {

  test("testLoadTrajectoryFromDataSource") {
    val sc = new SparkContext(new SparkConf().setAppName("TestLoader").setMaster("local"))
    val trajectoryLoader = TrajectoryLoader(sc)
    val inputPath = "hdfs://192.168.131.192:9000/data/xaa"
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)
    println(rdd.count())
  }

  test("testMapLine2Trajectory") {

  }

}
