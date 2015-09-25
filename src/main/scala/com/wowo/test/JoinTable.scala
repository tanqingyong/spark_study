package com.wowo.test



import org.apache.spark.{SparkConf, SparkContext}
object  JoinTable{


  def main(args: Array[String]) {
    if(args.length >= 2){
      val input = "hdfs://namenodens01/user/hive/warehouse/wowo_dw.db/fact_order_info"
      val input2 = "hdfs://namenodens01/user/hive/warehouse/wowo_dw.db/dim_goods"
      val output = "hdfs://namenodens01/user/tanqingyong/goods_sale"
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val order = sc.textFile(input).filter(_.contains("2014-08-31")).
              map(_.split("\001")).map(w=>Array(w(16).toInt,w(17).toInt,w(18).toDouble,w(19).toDouble))
      val goods = sc.textFile(input2).map(_.split("\001")).filter(_(1).toInt>1000000).map(w=>(w(1).toInt,w(3).toString))
      val tmp_goods_money = order.map(w=>(w(0).toInt,w(1)*w(2).toDouble)).reduceByKey((a,b)=>a+b)
      val tmp_goods_profit = order.map(w=>(w(0).toInt,w(1)*(w(2)-w(3)))).reduceByKey((a,b)=>a+b)
      val goods_result = goods.join(tmp_goods_money.join(tmp_goods_profit)).repartition(1)
      goods_result.map(w=>w._1+"\t"+w._2._1+"\t"+w._2._2._1+"\t"+w._2._2._2).saveAsTextFile(output)
    }else{
      System.exit(2)
    }
  }
}