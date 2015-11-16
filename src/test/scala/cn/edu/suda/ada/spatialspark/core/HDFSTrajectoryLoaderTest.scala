package cn.edu.suda.ada.spatialspark.core

import cn.edu.suda.ada.spatialspark.filters.OutlierDetection
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

    var sum = sc.accumulator(0)
    rdd.foreach(t => sum += t.GPSPoints.length)
    println("Before "+sum)

    val rdd2 = rdd.map(t => {
      val points = OutlierDetection.deleteOutlierPoints(t.GPSPoints,41)
      t.GPSPoints = points
      t
    })
     var sum2 = sc.accumulator(0)
    rdd2.foreach(t => sum2 += t.GPSPoints.length )
    println("After : "+sum2)
    //println(rdd.count())
//    val nrdd = rdd.foreach(t => {
//      for(i <- 1 to t.GPSPoints.length - 1){
//        val dis = t.GPSPoints(i).getDistance(t.GPSPoints(i-1))*1000
//        val interval = t.GPSPoints(i).timestamp - t.GPSPoints(i-1).timestamp
//        if(dis/interval > 41)println(dis/interval +" -> " + t.GPSPoints(i).speed)
//      }
//    })
//    val outPath = "hdfs://localhost:9000/data/xbb"
//    rdd.saveAsObjectFile(outPath)
    sc.stop()
  }

  test("testMapLine2Trajectory") {
    val sc = new SparkContext(new SparkConf().setAppName("TestLoader").setMaster("local"))
    val outPath = "hdfs://localhost:9000/data/xbb"
    val rdd = sc.objectFile(outPath,1)
    println("rdd count:"+rdd.count())
  }

}
