package com.citi.certification

/**
 * Created by kalit_000 on 20/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object SaprkStreamingWindowingMLLIB {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    def compute(input:RDD[String]) = input
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)

    def featurization(input:RDD[String]):RDD[LabeledPoint]= ???

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_MLLIB").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    ssc.checkpoint("check")

    val stream=ssc.socketTextStream("loclhost",9999)
    stream
      .window(Seconds(31))
      .transform(rdd => {
      val model=SVMWithSGD.train(featurization(rdd),12)
      rdd.map(input => (input,model))
    }).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
