package cn.edu.suda.ada.spatialspark.core

/**
 * The class GPSPoint is a representation of a GPS sample point by GPS sensor,which is typically a latLnt together with a timestamp.
 * Created by Graberial on 2015/8/12.
 */
class GPSPoint(val latitude:Float,val longitude:Float,val speed:Float,val timestamp:Long,val angle:Float){
  def this(latlng:Point,speed:Float,timestamp:Long,angle:Float){
    this(latlng.x,latlng.y,speed,timestamp,angle)
  }

  /**
   * @return Return a Point object
   */
  def getPoint():Point = new Point(latitude,longitude)
  /**
   * Getters and Setters
   */
//  def getLat:Double = latitude
//  def setLat(latitude:Double) { this.latitude = latitude}
//  def getLng:Double = y
//  def setLng(longitude:Double){y = longitude}
}
/**
 * companion object of class GPSPoint, with two apply methods for  instantiating GPSPoint object.
 */
object GPSPoint{

  /**
   * @param latitude  latitude of the GPS sample point
   * @param longitude longitude of the GPS sample point
   * @param speed      instantaneous  speed of the sample point
   * @param timestamp timestamp of the GPS sample point
   * @return new instance of class GPSPoint
   */
  def apply(latitude:Float,longitude:Float,speed:Float,timestamp:Long,angle:Float) = new GPSPoint(longitude,longitude,speed,timestamp,angle)

  /**
   * instantiate an GPSPoint object with a object point
   * @param latlng geographic coordinate of the sample point
   * @param speed  instantaneous  speed of the sample point
   * @param timestamp   timestamp of the GPS sample point
   * @return new instance of class GPSPoint
   */
  def apply(latlng:Point,speed:Float,timestamp:Long,angle:Float) = new GPSPoint(latlng,speed,timestamp,angle)
}