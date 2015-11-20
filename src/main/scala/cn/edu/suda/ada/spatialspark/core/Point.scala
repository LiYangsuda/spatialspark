package cn.edu.suda.ada.spatialspark.core
/**
 * A class that holds coordinates of a point.
 * Created by Graberial on 2015/8/12.
 */
class Point(var x:Float,var y:Float) {
  def equals(point:Point):Boolean = x == point.x && y == point.y

  override def toString ="(" + x + "," +  y + ")"

  /**
   * override method for hash code method
   * @return
   */
  override def hashCode = 13*x.hashCode+17*y.hashCode

}
object Point{
  def apply(x: Float,y:Float):Point = {
    new Point(x,y)
  }
  def apply(x: Double,y:Double):Point = {
    new Point(x.toFloat,y.toFloat)
  }

}