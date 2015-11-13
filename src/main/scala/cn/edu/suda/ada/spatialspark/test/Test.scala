package cn.edu.suda.ada.spatialspark.test

import cn.edu.suda.ada.spatialspark.core.{Trajectory, HDFSTrajectoryLoader, TrajectoryLoader, Worker}
import cn.edu.suda.ada.spatialspark.server.{JettyHttpServlet, JettyEmbedServer}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by liyang on 15-9-4.
 */


object Test {
  def main(args: Array[String]): Unit = {

  //val conf =  new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(
  //   Array(classOf[HDFSTrajectoryLoader], classOf[Trajectory]))

 // val sc = new SparkContext(conf.setMaster("local[8]").setAppName("Test"))
    val sc = new SparkContext(new SparkConf().set("spark.storage.memoryFraction", "0.8"))
    //set the input path of the data
    var inputPath = ""
//    if(args.length > 0){
//      inputPath = args(1)
//    }else{
     inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=0*/BASIS_TRAJECTORY*"
   // inputPath = "hdfs://192.168.131.192:9000/data/xaa"

    //inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=01/BASIS_TRAJECTORY_2015-r-00000,"+
      //"hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=02/BASIS_TRAJECTORY_2015-r-00000"
    Worker.setSparkContext(sc)
    val trajectoryLoader: TrajectoryLoader = TrajectoryLoader(sc)

    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)
    Worker.setRDDReference(rdd)
    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
    sc.stop()
  }
}

