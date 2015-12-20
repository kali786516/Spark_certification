package com.citi.certification

/**
 * Created by kalit_000 on 20/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object SparkStreamingWindowing {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    def compute(input:RDD[String]) = input
    .flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_app").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    ssc.checkpoint("check")

    val stream=ssc.socketTextStream("loclhost",9999)
    stream
      .window(Seconds(31),Seconds(1))
      .transform(rdd => compute(rdd))
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
