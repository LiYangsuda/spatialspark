package cn.edu.suda.ada.spatialspark.core

import org.scalatest.FunSuite

/**
 * Created by liyang on 15-10-5.
 */
class PointTest extends FunSuite {

  test("testY") {
    val p = new Point(12.4f,34.4f)
    assert(p.y == 34.3f)
  }

  test("testX") {

  }

  test("testToString") {

  }

  test("testEquals") {

  }

  test("testHashCode") {

  }

}
