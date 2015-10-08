package cn.edu.suda.ada.spatialspark.test

import cn.edu.suda.ada.spatialspark.server.{JettyEmbedServer,JettyHttpServlet}

import cn.edu.suda.ada.spatialspark.core.Worker
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by liyang on 15-9-4.
 */

object Test {
  def main(args: Array[String]): Unit = {
  //  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
    val sc = new SparkContext(new SparkConf())
    Worker.setSparkContext(sc)
    val inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000"
  //  val inputPath = "file:///home/liyang/Resources/xaa"
    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)
    //rdd.foreach(tra => println(tra.getTravelDistance+":"+tra.getDuration+":="+tra.getAverageSpeed))
   // val speed = rdd.map(tra => (tra.getAverageSpeed,1)).reduceByKey(_+_,1).sortByKey(true).collect()

    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
    sc.stop()
  }
}

