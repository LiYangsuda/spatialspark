package cn.edu.suda.ada.spatialspark.utils

import java.util.Random

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
    val inputPath = "hdfs://192.168.131.192:9000/data/xaa"
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)

    var sum = sc.accumulator(0)
    rdd.foreach(t => sum += t.GPSPoints.length)
    println("Before "+sum)

    val rdd2 = rdd.map(t => {
      val points = DouglasPeucker.compress(t.GPSPoints.toArray,0,t.GPSPoints.length-1,0.000000001)
      t.GPSPoints = points.toList
      t
    })
    var sum2 = sc.accumulator(0)
    rdd2.foreach(t => sum2 += t.GPSPoints.length )
    println("After : "+sum2)
  }

}
