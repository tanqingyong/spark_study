package com.wowo.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dean on 2015/9/22.
 */

object Simple{

  def properties()={

//    hdfs://namenodens01/user/hive/warehouse/wowo_dw.db/fact_order_info
//    hdfs://namenodens01/user/tanqingyong/goods_sale
  }

  def main(args: Array[String]) {
    if (args.length >= 2) {
//      val input = args(0)
//      val output = args(1)
      val input = "hdfs://namenodens01/user/hive/warehouse/wowo_dw.db/fact_order_info"
      val output = "hdfs://namenodens01/user/tanqingyong/goods_sale"
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val data = sc.textFile(input, 1)
      val lines = data.filter(_.contains("2014-08-30 12")).map(_.split("\001"))
      val line = lines.map((d) => (d(16).toInt, d(17).toInt, d(18).toDouble, d(19).toDouble))
      val result = line.map(w => (w._1, w._2 * w._3.toDouble, (w._3 - w._4) * w._2))

      val b = result.map((d) => (d._1.toInt, d._2.toDouble)).reduceByKey((a,b)=>a+b)
      val c = result.map((d) => (d._1.toInt, d._3.toDouble)).reduceByKey((a,b)=>a+b)
      val d = b.leftOuterJoin(c).repartition(1)

      d.map(w=>w._1+"\t"+w._2._1+"\t"+w._2._2).saveAsTextFile(output)
      // grep 1278329 00000*|grep "2014-08-30 12"|awk 'BEGIN{FS="\001";OFS="\t"}{print $3,$4,$16,$17,$18,$19,$20}'
    } else {

      System.exit(2)
    }
  }
}