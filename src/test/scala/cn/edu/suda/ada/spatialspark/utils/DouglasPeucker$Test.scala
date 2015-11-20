package cn.edu.suda.ada.spatialspark.utils

import cn.edu.suda.ada.spatialspark.core.{GPSPoint, TrajectoryLoader}
import cn.edu.suda.ada.spatialspark.filters.OutlierDetection
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by liyang on 15-11-16.
 */
class DouglasPeucker$Test extends FunSuite {

  test("testCompress") {
    val sc = new SparkContext(new SparkConf().setAppName("TestLoader").setMaster("local"))
    val trajectoryLoader = TrajectoryLoader(sc)
    val inputPath = "/home/liyang/Resources/xaa"
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)
    val trajectory = rdd.take(1)(0)
    println(trajectory.GPSPoints.length)
 //   val rdd1 = rdd.map(t => OutlierDetection.deleteOutlierPoints(t,45))
    val reduced = DouglasPeucker.reduction(trajectory,0.01)
    println(reduced.GPSPoints.length)
  }
}
