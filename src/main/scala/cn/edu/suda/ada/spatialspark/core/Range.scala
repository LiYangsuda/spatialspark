package cn.edu.suda.ada.spatialspark.core

/**
 * Created by liyang on 15-9-25.
 */
/**
 *A class the represents the a rectangle range
 */
class Range(val lngmin:Double,val latmax:Double,val lngmax:Double,val latmin:Double) extends Serializable{

  /**
   * Return the latitude distance
   */
  def getLatDistance = latmax - latmin

  /**
   *Return the longitude distance
   */
  def getLngDistance = lngmax - lngmin

  /**
   * Test whether this range contains the point.
   * @param point A random point.
   * @return  Return true if this range contains the point, false otherwise.
   */
  def contains(point:Point):Boolean = {
    contains(point.x,point.y)
  }
  def contains(lng:Float,lat: Float): Boolean ={
    lngmin < lng && lng < lngmax && latmin < lat && lat < latmax
  }
 override def toString = "Range:("+lngmin +","+latmax+","+lngmax+","+latmin+")"
}
object Range{
  /**
   * Initialize a Range object from String that consist of four coordinate, separated by comma.
   * @param range A String contains four parameters separated by comma
   */
  def apply(range:String): Range ={
    val res = if(validate(range)){
      val params = range.split(",")
      new Range(params(0).toDouble,params(1).toDouble,params(2).toDouble,params(3).toDouble)
    } else throw new Exception("arguments to instantiate object of Range is illegal")
    res
  }
  def apply(lngmin:Double,latmax:Double,lngmax:Double,latmin:Double): Range = {
    new Range(lngmin,latmax,lngmax,latmin)
  }
  /**
   * @todo
   * Validate whehter the string is valid to be transformed into a Range object. Note the result is set to be always true for testing
   * @param input The string to generate a Range object
   * @return
   */
  private def validate(input:String):Boolean = {
    true
  }
}
