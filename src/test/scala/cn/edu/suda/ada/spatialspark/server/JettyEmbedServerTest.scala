package cn.edu.suda.ada.spatialspark.server

import org.scalatest.FlatSpec

/**
 * Created by liyang on 15-10-21.
 */
class JettyEmbedServerTest extends FlatSpec {

  "Jetty server" should "start working after successfully launching" in{
    val serverName = "JettyEmbedServer"
    val port = 9999
    val jettyServer = new JettyEmbedServer(serverName, port)
    val servlet = new JettyHttpServlet()
    jettyServer.setServlet(servlet)
    jettyServer.start()
  }
}
