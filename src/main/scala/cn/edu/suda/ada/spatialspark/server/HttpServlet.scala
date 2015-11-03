package cn.edu.suda.ada.spatialspark.server

import java.io.PrintWriter
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import cn.edu.suda.ada.spatialspark.core.Worker
import org.apache.spark.Logging
/**
 * Created by liyang on 15-9-21.
 */
class JettyHttpServlet extends HttpServlet{
  private final  val serialVersionUID = 1L

  /**
   * The method doPost do nothing but write hello world to the client
   * @param req
   * @param resp
   */

  override def doPost(req:HttpServletRequest,resp:HttpServletResponse){

    resp.setContentType("text/html;charset=utf-8")
    resp.setHeader("Cache-Control","no-store")
    resp.setHeader("Pragma","no-cache")
    resp.setHeader("Connection","keep-alive")
    resp.setHeader("Access-Control-Allow-Origin","*")
    val filterParameters = getFilterMap(req)
    //val featureParameters = getFeatureMap(req)
    val features = getFeatures(req)
    val featureParameters = getProperLevelStep(features,filterParameters)
    // Print out the parameters in the Http request. For testing purpose only
    if(filterParameters != null){
      for(param <- filterParameters){
        System.out.println(param.toString())
      }
      Worker.applyFilters(filterParameters)
    }
    else{
      System.out.println("Empty filters")
    }

    if(featureParameters != null){
      for(feature <- featureParameters){
        System.out.println(feature)
      }
    }

    /*features are in the format of Map[featureName:String,featureDistribution: Array[(Int,Int)], where the first parameter represents the feature name while the second
      parameter represents the distribution of the feature. Feature distribution is stored in the data structure of Array[Tuple2(lowBound:Int,numbers:Int)]. Here lowBound represent
     the low bound of a range and numbers represents the number of trajectories that fall into that range. For example, if the passed parameter level step is 2 and a tuple in the
     distribution is (0,1111), that means there are 1111 trajectories that fall into the range [0,2)
    */
//
//    val distributions = Worker.calculateFeatures(featureParameters)
//
//    featureDisplay(distributions)
//    //System.out.println(Worker.toJson(distributions,featureParameters))
//    var out: PrintWriter = null
//    try{
//      out = resp.getWriter
//      out.print(Worker.toJson(distributions,featureParameters))
//      out.flush()
//    }catch {
//      case e: Exception => System.out.println(e.printStackTrace())
//    }finally {
//      out.close()
//    }
  }
  override def doGet(req:HttpServletRequest,resp:HttpServletResponse){

    doPost(req,resp)
  }

  /**
   * Get expected feature parameters from request
   * @param request Http request from client
   * @return Parameters of expected feature.Type:Map[featureName:String,featureLevelStep:Int]
   */
  @deprecated def getFeatureMap(request: HttpServletRequest):Map[String,Int] = {
    var parameterMap = Map[String,Int]()
    val params = request.getParameter("features")
    if(params != null && params.contains(",")){
      val paramArray = params.split(",")
      for(key <- paramArray){
        parameterMap += (key -> request.getParameter("feature."+key+".levelStep").toInt)
      }
    } else{
      parameterMap += (params -> request.getParameter("feature."+params+".levelStep").toInt)
    }
    parameterMap
  }

  def getFeatures(request: HttpServletRequest):Array[String] = {
    val params = request.getParameter("features")
    var paramsArray = Array[String]()
    if(params.contains(",")){
      paramsArray = params.split(",")
    }else{
      paramsArray = Array(params)
    }
    paramsArray
  }

  /**
   * Get all filter parameters from HTTP request. All this parameters will be applied to trajectory data by Worker.
   * @param request HttpServletRequest
   * @return  All filters in the format of Map[filterName:String,Map[key:String,value:String] ].
   */
  def getFilterMap(request: HttpServletRequest):Map[String,Map[String,String]] = {
    var parameterMap = Map[String, Map[String, String]]()
    val params = request.getParameter("filters")
    if (params == null) {
      parameterMap = null
    }else {
      val paramArray = if (params.contains(",")) params.split(",") else Array(params)

      for (key <- paramArray) {
        var filterParams = Map[String, String]() //Save the filter of one filter
        //OTime,DTime,TravelTime,AvgSampleTime  all have the same format of  :filter.[FilterName].time:value,filter.[FilterName].relation:relation
        if (key.equalsIgnoreCase("OTime") || key.equalsIgnoreCase("DTime") || key.equalsIgnoreCase("TravelTime") || key.equalsIgnoreCase("AvgSampleTime")) {
          filterParams += ("value" -> request.getParameter("filter." + key + ".time"))
          filterParams += ("relation" -> request.getParameter("filter." + key + ".relation"))
        }
        //TravelDistance
        if (key.equalsIgnoreCase("TravelDistance")) {
          filterParams += ("value" -> request.getParameter("filter." + key + ".dis"))
          filterParams += ("relation" -> request.getParameter("filter." + key + ".relation"))
        }
        //AvgSpeed
        if (key.equalsIgnoreCase("AvgSpeed") || key.equalsIgnoreCase("SimpleSpeed")) {
          filterParams += ("value" -> request.getParameter("filter." + key + ".speed"))
          filterParams += ("relation" -> request.getParameter("filter." + key + ".relation"))
        }
        /*Filters, such as OPoint, DPoint,PassRange and Range, all have the same format:
          1. filter.[FilterName].minLat 最小的纬度范围
          2. filter.[FilterName].maxLat 最大的纬度范围
          3. filter.[FilterName].minLng 最小的经度范围
          4. filter.[FilterName].maxLng 最大的经度范围
        */
        if (key.equalsIgnoreCase("OPoint") || key.equalsIgnoreCase("DPoint") || key.equalsIgnoreCase("PassRange") || key.equalsIgnoreCase("Range")) {
          filterParams += ("minLat" -> request.getParameter("filter." + key + ".minLat"))
          filterParams += ("maxLat" -> request.getParameter("filter." + key + ".maxLat"))
          filterParams += ("minLng" -> request.getParameter("filter." + key + ".minLng"))
          filterParams += ("maxLng" -> request.getParameter("filter." + key + ".maxLng"))
        }
        parameterMap += (key -> filterParams)
      }
    }
    parameterMap
  }

  /**
   * Instead of letting the user specify the levelstep of each feature without an intuition of what the output will be like,
   * we decide to calculate a proper levelstep for each feature. In general, if a filter concerning the feature is applied to
   * the data, then we will divide the output into 20 levelsteps. Otherwise, a default value is suggested.
   * */
   def getProperLevelStep(features: Array[String],filters: Map[String,Map[String,String]]): Map[String,Int]= {
    var parameterMap = Map[String,Int]()
    for(feature <- features){
      println(feature)
      var levelStep = 1
      feature match {
        case "TrajAvgSpeed" => {
          if(filters != null){
            val filter = filters.get("AvgSpeed")
            if(filter != null){                   //if avgspeed filter exists
              if(filter.get("relation") == "lt"){
                levelStep =  filter.get("value").toInt / 20
              }else{
                levelStep =   Math.abs(100 - filter.get("value").toInt) / 20
              }
            }
            else {
              levelStep = 5 //default levelstep
            }
          }else {
            levelStep = 5 //default levelstep
          }
        }
        case "TrajTravelDistance" => {
          if(filters != null){
            val filter = filters.get("TravelDistance")
            if(filter != null){                   //if avgspeed filter exists
              if(filter.get("relation") == "lt"){
                levelStep =    filter.get("value").toInt / 20
              }else{
                levelStep =     Math.abs(200000 - filter.get("value").toInt) / 20
              }
              println(levelStep)
            }else{
              levelStep =    10000             //default levelstep
            }
          }
          else {
            levelStep = 10000 //default levelstep
          }
        }
        case "TrajTravelTime" => {
          if(filters != null){
            val filter = filters.get("TravelTime")
            if(filter != null){                   //if avgspeed filter exists
              if(filter.get("relation") == "lt"){
                levelStep =    filter.get("value").toInt / 20
              }else{
                levelStep =    Math.abs(36000 - filter.get("value").toInt) / 20
              }
            }else{
              levelStep =   900
            }
          }
          else {
            levelStep = 900 //default levelstep
          }
        }
        case "TrajSamplePointsCount" => {
          levelStep = 200
        }
        case "TrajAvgSampleTime" => {
          if(filters != null){
            val filter = filters.get("AvgSampleTime")
            if(filter != null){                   //if avgspeed filter exists
              if(filter.get("relation") == "lt"){
                levelStep =      filter.get("value").toInt / 20
              }else{
                levelStep =     Math.abs(200 - filter.get("value").toInt) / 20
              }
            }else{
              levelStep =    50
            }
          }
          else {
            levelStep = 50 //default levelstep
          }
        }
        case "GPSSampleSpeed" => {
          levelStep =    5
        }
        case _ => levelStep =   1000
      }
      parameterMap += (feature -> levelStep)
    }
    parameterMap
  }
  /**
   * for testing only
   * @param dis
   */
  @volatile def featureDisplay(dis: Map[String,Array[(Int,Int)]]): Unit ={
    dis.foreach(d =>{
      d._2.foreach(println _)
    })
  }
}
