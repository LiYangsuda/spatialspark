package cn.edu.suda.ada.spatialspark.server


import javax.servlet.http.HttpServlet
import javax.servlet.{Servlet, Filter}
import org.apache.spark.Logging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.servlet._
import org.eclipse.jetty.util.thread.QueuedThreadPool

/**
 * Created by liyang on 15-9-19.
 */
private [spatialspark] class JettyServerStateException(message:String) extends Exception(message)
/**
 * A class the wrap around the jetty server and serves as the HTTP server that accept request from the client and extract the parameters from the
 * request and then forward the request to spark cluster. It also responsible for  responding the requests with computed results in json format.
 */
class JettyEmbedServer(val serverName:String,var port:Int=0,val contextPath:String = "/",val threadNum:Int=50) extends Logging{
  var filters = Map[String,Filter]()
  var servlets = Map[String,Servlet]()
  private var server:Server = null

  /**
   * Initialize the server
   */
  private def init(){
    //If the server is already running, throw an exception
    if(server != null){
      throw new JettyServerStateException("server already exists")
    }else{
      server = new Server()
      //Set the server connectors parameters
      val connector:SelectChannelConnector = new SelectChannelConnector
      connector.setPort(port)
      connector.setMaxIdleTime(60*1000)
      connector.setRequestHeaderSize(8192)
      val threadPool: QueuedThreadPool = new QueuedThreadPool(threadNum)
      threadPool.setName("embed-jetty-http")
      connector.setThreadPool(threadPool)

      server.setConnectors(Array(connector))
      val context: ServletContextHandler = new ServletContextHandler(server,contextPath)

//      //add filters
//      if(filters != null){
//        for(filter <- filters){
//          logInfo("add filter = "+filter._1)
//          context.addFilter(new FilterHolder(filter._2.getClass),filter._1,FilterMapping.DEFAULT)
//        }
//      }

      //add servlet
      if(servlets != null){
        for(servlet <- servlets){
          logInfo("add servlet = "+servlet._1)
          if(servlet == null) logInfo("null servlet")
          context.addServlet(new ServletHolder(servlet._2.getClass),"/")
        }
      }
    }
  }

  /**
   * This method actually start the server
   */
  def start(): Unit ={
    init()
    server.start()

    logInfo("jetty embed server started on port = "+port)
  }

  def stopServer(): Unit ={
    server.stop()
    server.destroy()
  }

  def setFilters(filters:Map[String,Filter]): Unit ={
    this.filters = filters
  }

  def setServlet(servlet:HttpServlet): Unit ={
    this.servlets += ("httpServlet" -> servlet)
  }
  def setServlets(servlets:Map[String,Servlet])={
    this.servlets = servlets
  }

  /**
   * Users can invoke this method to initialize and  start the server
   */
  def execute(): Unit ={
//    Runtime.getRuntime.addShutdownHook(new Thread(){
//      override def run(): Unit ={
//        try{
//          stopServer()
//        }catch {
//          case ex: Exception => logError(ex.toString)
//        }
//      }
//    })
    try{
      start()
    }catch {
      case e: Exception => logError("start JettyEmbedServer error " + e.toString)
    }
  }
}
