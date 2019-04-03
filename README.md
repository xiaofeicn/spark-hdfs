使用
=======
* 
     submit 命令--class 指定为 com.cj.spark.streaming.streaming.StartStreaming 并传入参数 `test|dev|uat` 任一
     其他配置详见 package com.cj.uti.config.properties
     


packages com
=======

package com.cj.spark.streaming.streaming
---
 
+ 1 'StartStreaming'
   -  作用：
        ```
        流处理执行入口
        ```
        
   -  参数：
        ```
        env：test|dev|uat
        ```

+ 2 'DisposeHDFSStream'
   -  作用
        ```
        流处理主体,监控hdfs路径，监控路径由env参数决定
        ```
   -  参数：
        ```
        checkpointDirectory：
        appName：流处理程序名
        env：test|dev|uat
        ```

+ 3 'DisposeRabbit'
   -  作用
        ```
        流处理主体，接收rabbitmq消息[已弃用]
        ```
   -  参数：
        ```
        appName：流处理程序名
        ```

package com.cj.spark.streaming.models
---
+ 1 'DisposeRabbit'
   -  作用
        ```
        类表，创建DataFrame
        ```

package com.cj.util
---
+ 1 'config.properties'
   -  作用
        ```
        配置文件
        ```
+ 2 'ConfigerHelper'
   -  作用
        ```
        获取配置文件内item
        ```
+ 3 'DBHelper'
   -  作用
        ```
        获取配置文件内item
        ```
   -  参数：
        ```
        env：test|dev|uat
        ```
+ 4 'GraceCloseHelper'
   -  作用
        ```
        Streaming守护线程，用户停止流处理
        ```
   -  参数：
        ```
        StreamingContext
        ```    

packages org
---
* 
    Streaming-RabbitMq  源码
      