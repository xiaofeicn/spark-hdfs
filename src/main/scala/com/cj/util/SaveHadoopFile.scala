package com.cj.util

import java.io.{BufferedWriter, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object SaveHadoopFile {
  private[this] val HDFSNoPass = ConfigerHelper.getProperty("hdfs.nopas")
  val dateForMater: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def save(saveString:String,appName:String): String ={

    val toDay = dateForMater.format(new Date())
    val path = s"/$appName/nopass/$toDay"
    val config = new Configuration()
    config.addResource(new Path("src/main/resources/hdfs-site.xml"))
    val fs: FileSystem = FileSystem.get(config)
    val save_path=new Path(path+"/"+new Date().getTime)
    val out=fs.create(save_path)
    var writer:BufferedWriter =null
    try {
      writer = new BufferedWriter(new OutputStreamWriter(out))
      writer.write(saveString)

    }finally {
      writer.flush()
      writer.close()
      out.close()
    }
    "文件路径:"+save_path+",\t已存json数据:"+saveString

  }
  def save_path(appName:String,env:String): String ={
    val HDFSNoPass = ConfigerHelper.getProperty(s"hdfs.nopas.$env")
    val toDay = dateForMater.format(new Date())
    s"$HDFSNoPass/$appName/$env/nopass/$toDay"

  }


}
