package cn.edu.suda.ada.spatialspark.server

import org.scalatest.FunSuite

/**
 * Created by liyang on 15-11-3.
 */
class JettyHttpServletTest extends FunSuite {

  test("testGetProperLevelStep") {
    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
  }

}
