package com.atwy

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val starttime=System.currentTimeMillis()
    val config = new SparkConf().setMaster("local[2]").setAppName("WC")
    val stx = new SparkContext(config)
    val fileRdd = stx.textFile("C://Users//yan//Desktop//大数据相关//sparkfile//spark.txt")
    val words = fileRdd.flatMap(line => line.split(" "))
    val wordTupe = words.map((_, 1))
    wordTupe.reduceByKey(_ + _)
            .collect()
            .foreach(println)
    val endtime=System.currentTimeMillis()
    println("处理完成，耗时： "+(endtime-starttime) +" ms")
    stx.stop()
  }
}
