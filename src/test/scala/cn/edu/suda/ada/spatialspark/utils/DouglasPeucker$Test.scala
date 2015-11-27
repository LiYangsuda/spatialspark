package cn.edu.suda.ada.spatialspark.utils

import java.io.FileWriter

import cn.edu.suda.ada.spatialspark.core.{GPSPoint, TrajectoryLoader}
import cn.edu.suda.ada.spatialspark.filters.OutlierDetection
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by liyang on 15-11-16.
 */
class DouglasPeucker$Test extends FunSuite {

  test("testCompress") {
    val sc = new SparkContext(new SparkConf().setAppName("TestLoader").setMaster("local"))
    val trajectoryLoader = TrajectoryLoader(sc)
    val inputPath = "/home/liyang/Resources/xai"
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)
    val trajectory = rdd.take(1)(0)
    new FileWriter("/home/liyang/Resources/out.txt").write(trajectory.GPSPoints.mkString(","))
    println(trajectory.GPSPoints.length)
 //   val rdd1 = rdd.map(t => OutlierDetection.deleteOutlierPoints(t,45))
    val reduced = DouglasPeucker.reduction(trajectory,50)

    println(reduced.GPSPoints.length)
  }

  test("test compression"){
    val input = "/home/liyang/Resources/out.txt"
    val lines = Source.fromFile(input).getLines()
    val points = ArrayBuffer[GPSPoint]()
    for(it <- lines){
      val arr = it.split(",")
      points +=  GPSPoint(arr(0).toFloat,arr(1).toFloat,0.0f,0,0)
    }

    DouglasPeucker.reduction(points.toArray,50)
  }
}
