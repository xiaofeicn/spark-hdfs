package com.cj.util

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.spark_project.jetty.server.{Request, Server}
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler}

object GraceCloseHelper {

  lazy val log = LogManager.getLogger("GraceCloseUtils")

  /**
    * 1. HTTP方式
    * 负责启动守护的jetty服务
    * @param port 对外暴露的端口号
    * @param ssc Stream上下文
    */
  def daemonHttpServer(port:Int, ssc: StreamingContext) = {
    val server = new Server(port)
    val context = new ContextHandler()
    context.setContextPath("/close")
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  def daemonHttpServerEnv(env:String, ssc: StreamingContext) = {
    var port=55551
    if("test".equals(env)){
      port=55552
    }
    else if("dev".equals(env)){
      port=55553
    }
    val server = new Server(port)
    val context = new ContextHandler()
    context.setContextPath("/close")
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  /**
    * 负责接受http请求来优雅的关闭流
    * @param ssc Stream上下文
    */
  class CloseStreamHandler(ssc:StreamingContext) extends AbstractHandler {
    override def handle(s: String, baseRequest: Request, req: HttpServletRequest, response: HttpServletResponse): Unit = {
      log.warn("开始关闭......")
      // 优雅的关闭
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      response.setContentType("text/html; charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)
      val out = response.getWriter
      out.println("Close Success")
      baseRequest.setHandled(true)
      log.warn("关闭成功.....")
    }

  }
}
