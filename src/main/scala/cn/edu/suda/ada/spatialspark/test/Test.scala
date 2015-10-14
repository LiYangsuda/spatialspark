package cn.edu.suda.ada.spatialspark.test

import cn.edu.suda.ada.spatialspark.server.{JettyEmbedServer,JettyHttpServlet}

import cn.edu.suda.ada.spatialspark.core.Worker
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by liyang on 15-9-4.
 */

object Test {
  def main(args: Array[String]): Unit = {
   // val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
    val sc = new SparkContext(new SparkConf())
    Worker.setSparkContext(sc)

    //set the input path of the data
    var inputPath = ""
//    if(args.length > 0){
//      inputPath = args(1)
//    }else{
//      //inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=0*/BASIS_TRAJECTORY*"
//     // inputPath = "hdfs://192.168.131.192:9000/data/xaa"
//      inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000," +
//        ",hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=02/BASIS_TRAJECTORY_2015-r-00000,"+
//        "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=03/BASIS_TRAJECTORY_2015-r-00000"
//    }

    inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000," +
      "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=02/BASIS_TRAJECTORY_2015-r-00000,"+
      "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=03/BASIS_TRAJECTORY_2015-r-00000"

    val rdd = Worker.loadTrajectoryFromDataSource(inputPath)

    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
    sc.stop()
  }
}

