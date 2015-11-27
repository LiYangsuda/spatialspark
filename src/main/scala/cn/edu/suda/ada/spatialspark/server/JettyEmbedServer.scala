package cn.edu.suda.ada.spatialspark.server


import java.io.File
import java.net.ServerSocket
import javax.servlet.http.HttpServlet

import org.apache.log4j.LogManager
import org.apache.spark.Logging

import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.{Connector, Server}
import org.eclipse.jetty.servlet._
import org.eclipse.jetty.util.thread.{ThreadPool, QueuedThreadPool}

/**
 * Created by liyang on 15-9-19.
 */
//public class JettyServerStateException(message:String) extends Exception(message)
/**
 * A class the wrap around the jetty server and serves as the HTTP server that accept request from the client and extract the parameters from the
 * request and then forward the request to spark cluster. It also responsible for  responding the requests with computed results in json format.
 */
class JettyEmbedServer(var serverName:String,var port : Int,val baseDir : String) extends Logging{

  private var server : Server = null
  private var servlet: HttpServlet = null
  //Test whether the passed port has been used
  try{
    val socket = new ServerSocket(port)
    socket.close()
  }catch {
    case e : Exception => {
      logError("port :" + port + " is been used")
      throw new IllegalArgumentException("port:"+port +" is used in "+this)
    }
  }
  //If the baseDir is provided, then test whether it is a valid directory.
  val file = new File(System.getProperty("user.dir")+baseDir)
  if(file.exists()){
    if(!file.isDirectory() || !file.canWrite() || !file.canRead){
      logError("provide base directory illegal! system is going to shut down")
      throw new IllegalArgumentException("provided base directory is illegal")
    }
  }else{  //If provided directory is not exist, then create it
    try{
      file.mkdir()
    }
  }
  def this(serverName:String,port:Int){
    //If the working directory for the server is not provided, then /tmp/jetty is set to be the default directory.
    this(serverName,port,"/")
  }

  def this(port:Int){
    this("default server",port)
  }
  def this(){
    this(8088)
  }
  /**
   * Initialize the server
   */
  private def init(){
    //If the server is already running, throw an exception
    if(server != null){
      logError("server already exists")
      System.exit(-1)
    }else{
      server = new Server()

      //Set the server connectors parameters
      val connector = new SelectChannelConnector()
      connector.setPort(port)
      connector.setMaxIdleTime(60*1000)
      server.setConnectors(Array(connector))
      val threadPool = new QueuedThreadPool(20)
      threadPool.setName("embed-jetty-http")

      connector.setThreadPool(threadPool)


      val context  = new ServletContextHandler(server,"/")

      /*add the servlet for handling ajax request from the client. Before that, the servlet must be initialized first, or
      an exception will be thrown*/
      if(servlet == null) throw new NullPointerException("servlet is null in JettyEmbedServer")

      context.addServlet(new ServletHolder(servlet.getClass),"/spatialFeature")  //This servlet is designed to handled all post request
    }
  }

  /**
   * This method actually start the server
   */
  def start(): Unit ={
    init()
    doStart()
  }

  def stopServer(): Unit ={
    logInfo("jetty server is going to shut down now")
    server.stop()
    server.destroy()
  }


  def setServlet(servlet:HttpServlet): Unit ={
    this.servlet = servlet
  }

  /**
   * Users can invoke this method to initialize and  start the server
   */
  def doStart(): Unit ={
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit ={
        try{
          stopServer()
        }catch {
          case ex: Exception => logError(ex.toString)
        }
      }
    })
    try{
      server.start()
      logInfo("jetty started")
      server.join()
    }catch {
      case e: Exception => logError("start JettyEmbedServer error " + e.toString)
    }
  }
}
