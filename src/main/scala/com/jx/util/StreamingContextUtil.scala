package com.jx.util

import org.apache.spark.streaming.StreamingContext
import org.slf4j.{Logger, LoggerFactory}

object StreamingContextUtil{
  val logger: Logger = LoggerFactory.getLogger(StreamingContextUtil.getClass)
  private var streamingContext: StreamingContext = _

  def init(streamingContext: StreamingContext): Unit ={
    this.streamingContext = streamingContext
  }

  def getStreamingContext: StreamingContext = {
    if(null == streamingContext){
      logger.error("StreamingContextUtil streamingContext instance empty")
    }
    streamingContext
  }

}
