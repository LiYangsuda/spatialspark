package cn.edu.suda.ada.spatialspark.core

/**
 * A class that holds coordinates of a rectangle.
 * Created by Graberial on 2015/8/13.
 */
class Rectangle(private var upperLeft:Point,private var bottomRight:Point) {
  /**
   *Formations of GPS point varies,for instance,using N&S or positive&negative numbers to distinguish the north and the south.
   * In our implementation, GPS points are represented in  Gauss�CKr��ger coordinate system
   * @param x1 left boundary
   * @param y1 top boundary
   * @param x2 right boundary
   * @param y2 bottom boundary
   */
  def this(x1:Float,y1:Float,x2:Float,y2:Float){
    this(new Point(x1,y1),new Point(x2,y2))
  }

  /**
   * @param that another Rectangle object
   * @return True if the two rectangles' have the same coordinate, otherwise false
   */
  def equals(that:Rectangle):Boolean = upperLeft.equals(that.upperLeft) && bottomRight.equals(that.bottomRight)

  /**
   * Test weather this rectangle area covers the given point
   * Note that the GPS points are represented in  Gauss�CKr��ger coordinate system.
   * @param x x axis of the given point for testing
   * @param y y axis of the given point for testing
   * @return True if the point is covered by the rectangle ,otherwise false
   */
  def contains(x:Float,y:Float):Boolean = upperLeft.x < x && x < bottomRight.x &&  bottomRight.y < y && y < upperLeft.y

  /**
   * Getters and setters
   */
  def getLeft = upperLeft.x
  def setLeft(left:Float) {upperLeft.x = left}
  def getRight = bottomRight.x
  def setRight(right:Float){bottomRight.x = right}
  def getTop = upperLeft.y
  def setTop(top:Float){upperLeft.y = top}
  def getBottom = bottomRight.y
  def setBottom(bottom:Float){bottomRight.y = bottom}

  /*
   * Get the height or width of this rectangle
   */
  def getHeight = upperLeft.y - bottomRight.y
  def getWidth = bottomRight.x - upperLeft.x
  override def toString = "Rectangle::UpperLeft: "+upperLeft.toString+" BottomRight: "+bottomRight.toString
}