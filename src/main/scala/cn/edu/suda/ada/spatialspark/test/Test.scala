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

  //Use KryoSerializer
  //val conf =  new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(
  //   Array(classOf[HDFSTrajectoryLoader], classOf[Trajectory]))

   val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Test"))          //Local testing
//    val sc = new SparkContext(new SparkConf().set("spark.storage.memoryFraction", "0.8"))     // For running on the cluster

    //set the input path of the data
    var inputPath = ""

   // inputPath = "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=0*/BASIS_TRAJECTORY*"   //All trajectories
    //inputPath = "hdfs://192.168.131.192:9000/data/xaa"                                              //100 trajectories for local testing
    inputPath = "/home/liyang/Resources/xai"
   // inputPath = "/home/liyang/Resources/BASIS_TRAJECTORY_2015-r-00000"                              //4w trajectories for local testing
   // inputPath =  "hdfs://node1:9000/data/GaoTong/BasicTrajectory/201504/m=04/d=02/BASIS_TRAJECTORY_2015-r-00000"　　//4w trajectory on hdfs
    Worker.setSparkContext(sc)
    val trajectoryLoader: TrajectoryLoader = TrajectoryLoader(sc)

    // Load trajectory data from text file
    val rdd = trajectoryLoader.loadTrajectoryFromDataSource(inputPath)

    //Delete outlier points in a trajectory
//    val nRdd = rdd.map(t => OutlierDetection.deleteOutlierPoints(t,41))

    //Compress the GPS points in a trajectory
    val nRdd = rdd.map(t =>{
    DouglasPeucker.reduction(t,0.017)
    })
    println(nRdd.count())
    Worker.setRDDReference(nRdd)

    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
    sc.stop()
  }
}

