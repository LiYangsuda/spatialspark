package cn.edu.suda.ada.spatialspark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import cn.edu.suda.ada.spatialspark.core.Worker


/**
 * Created by liyang on 15-9-4.
 */

object Test {

  def main(args: Array[String]): Unit = {

    //    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Test"))
    //    val inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000"
    //    val lines = sc.textFile(inputPath)
    //
    //    val start = System.currentTimeMillis()
    //    val trajectoryRDD = lines.map(HDFSTrajectoryLoader.mapLine2Trajectory).persist()
    //    val trajectoryCounts = trajectoryRDD.count()
    //    val end = System.currentTimeMillis(
    //    println("Total trajectory number is :"+trajectoryCounts+"\n TIME spend is :"+(end - start))
    //    val serverName = "JettyEmbedServer"
    //    val port = 4321
    //    val jettyServer = new JettyEmbedServer(serverName,port)
    //    val servlet = new JettyHttpServlet()
    //    jettyServer.setServlet(servlet)
    //    jettyServer.execute()
    //    val levelStep = 2
    //
    //    val distribution =  getFeatures(trajectoryRDD,TrajectoryAverageSpeedClassifier.getLevel,levelStep)
    //    distribution.foreach(println _)
    //    sc.stop()
    //

    val sc = new SparkContext(new SparkConf().setMaster("spark://node1:7077").setAppName("IDETest"))
    Worker.setSparkContext(sc)
    val inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000"
    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)
    val speed = rdd.map(tra => (tra.getAverageSpeed,1)).reduceByKey(_+_,1).sortByKey(true).collect()
    speed.foreach(println _)
//    val serverName = "JettyEmbedServer"
//    val port = 9999
//    val jettyServer = new JettyEmbedServer(serverName, port)
//    val servlet = new JettyHttpServlet()
//    jettyServer.setServlet(servlet)
//    jettyServer.execute()
//    while (true){
//      Thread.sleep(10);
//    }
    sc.stop()
  }
}
