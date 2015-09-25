package com.yong.qing

/**
 * Created by dean on 2015/8/25.
 */

import org.apache.spark.{SparkConf, SparkContext}


object Test {

def main(args:Array[String]):Unit={
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val arr = Array(2,3,45,6,0,8)
  val data = sc.parallelize(arr).map(_*2).collect()
  data.foreach(println(_))
//  val txt = sc.textFile("README.md",2)
//  txt.filter(_.contains("haha")).count()
  }
}
