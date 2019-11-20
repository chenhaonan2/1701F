package com.foo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder


object Spadkafka {
  def main(args: Array[String]): Unit = {
   System.setProperty("hadoop.home.dir", "D:\\anzhaung\\hsdoop-2.6.0")
   
   val conf = new SparkConf
   conf.setAppName(Spadkafka.getClass.getSimpleName)
   conf.setMaster("local[*]")
   
   val streamContext  = new StreamingContext(conf,Seconds(5))
   streamContext.checkpoint("E:/che.txt")
   
   
   val chenhaonan = Array("chenhaonan").toSet
   val kafkaParams = Map[String,String]("metadata.broker.list"->"linux001:9092") 
  
   val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamContext,kafkaParams,chenhaonan)
   
   stream.foreachRDD(rdd=>{
     rdd.foreach(line=>{
       println("key="+ line._1+"  value="+ line._2)
     })
    
   })
   streamContext.start()
   streamContext.awaitTermination();
  }
}