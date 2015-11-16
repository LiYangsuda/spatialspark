package cn.edu.suda.ada.spatialspark.test

import cn.edu.suda.ada.spatialspark.core.{Trajectory, HDFSTrajectoryLoader, TrajectoryLoader, Worker}
import cn.edu.suda.ada.spatialspark.filters.OutlierDetection
import cn.edu.suda.ada.spatialspark.server.{JettyHttpServlet, JettyEmbedServer}
import cn.edu.suda.ada.spatialspark.utils.DouglasPeucker
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

    // Load trajectory data from text file
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)

    //Delete outlier points in a trajectory
    val nRdd = rdd.map(t => OutlierDetection.deleteOutlierPoints(t,41))

    //Compress the GPS points in a trajectory
    val nRdd2 = nRdd.map(t =>{
      val points = DouglasPeucker.compress(t.GPSPoints.toArray,0,t.GPSPoints.length-1,0.00001)
      t.GPSPoints = points.toList
      t
    })
    println(nRdd2.count())
    Worker.setRDDReference(nRdd2)

    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
    sc.stop()
  }
}

