package com.cj.spark.streaming.streaming

import com.cj.util.ConfigerHelper
import com.cj.spark.streaming.streaming.DisposeHDFSStream.createStreamingContext
import org.apache.log4j.Logger
import com.cj.util.GraceCloseHelper.daemonHttpServerEnv
import org.apache.spark.streaming.StreamingContext

object StartStreaming {
  private[this] val checkpointDirectory = ConfigerHelper.getProperty("checkpointDirectory")
  private[this] val appName = ConfigerHelper.getProperty("appName")
  private[this] val thisCheckpointDirectory = checkpointDirectory + appName
  val log: Logger = org.apache.log4j.LogManager.getLogger(appName)

  /**
    * streaming 启动 参数为运行环境，test|dev|uat
    * @param args
    */

  def main(args: Array[String]): Unit = {
    /**
      * 使用Checkpoint
      * val sct = StreamingContext.getOrCreate(thisCheckpointDirectory,
      * () => {
      * createStreamingContext(thisCheckpointDirectory, appName)
      * })
      *
      */

      if (args.length!=1){
        log.error("参数错误")

        System.exit(1)
      }
    val env=args(0)
    log.info(s"=========运行环境  $env======")
    val sct = createStreamingContext(thisCheckpointDirectory, s"$appName - $env",env)
    sct.start()
    daemonHttpServerEnv(env, sct)
    sct.awaitTermination()

  }
}
