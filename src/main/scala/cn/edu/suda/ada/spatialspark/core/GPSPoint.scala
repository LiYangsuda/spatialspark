package cn.edu.suda.ada.spatialspark.core

/**
 * The class GPSPoint is a representation of a GPS sample point by GPS sensor,which is typically a latLnt together with a timestamp.
 * Created by Graberial on 2015/8/12.
 */
class GPSPoint(val longitude:Float,val latitude:Float,var speed:Float,val timestamp:Long,val angle:Short) extends Serializable{
  def this(latlng:Point,speed:Float,timestamp:Long,angle:Short){
    this(latlng.x,latlng.y,speed,timestamp,angle)
  }

  /**
   * @return Return a Point object
   */
  def getPoint():Point = new Point(longitude,latitude)

  /**
   * Get the distance between two gps points
   * @param other another gps point
   * @return kilometers
   */
  def getDistance(other: GPSPoint):Double={
    val lat1 = (Math.PI/180)*latitude
    val lat2 = (Math.PI/180)*other.latitude
    val lon1 = (Math.PI/180)*longitude
    val lon2 = (Math.PI/180)*other.longitude
    val R = 6371
    val distance = Math.acos(Math.sin(lat1)*Math.sin(lat2)+Math.cos(lat1)*Math.cos(lat2)*Math.cos(lon2-lon1))*R
    distance
  }
  /**
   * Getters and Setters
   */
//  def getLat:Double = latitude
//  def setLat(latitude:Double) { this.latitude = latitude}
//  def getLng:Double = y
//  def setLng(longitude:Double){y = longitude}
  override def toString = "Lng: "+longitude +" Lat: " + latitude +" Speed: " + speed + " Timestamp: "+ timestamp
}
/**
 * companion object of class GPSPoint, with two apply methods for  instantiating GPSPoint object.
 */
object GPSPoint{

  /**
   * @param longitude longitude of the GPS sample point
   * @param latitude  latitude of the GPS sample point
   * @param speed      instantaneous  speed of the sample point
   * @param timestamp timestamp of the GPS sample point
   * @return new instance of class GPSPoint
   */
  def apply(longitude:Float,latitude:Float,speed:Float,timestamp:Long,angle:Short) = new GPSPoint(longitude,latitude,speed,timestamp,angle)

  /**
   * instantiate an GPSPoint object with a object point
   * @param latlng geographic coordinate of the sample point
   * @param speed  instantaneous  speed of the sample point
   * @param timestamp   timestamp of the GPS sample point
   * @return new instance of class GPSPoint
   */
  def apply(latlng:Point,speed:Float,timestamp:Long,angle:Short) = new GPSPoint(latlng,speed,timestamp,angle)
  def apply(longitude: Float,latitude:Float):GPSPoint = new GPSPoint(longitude,latitude,0.0f,0L,0)
}