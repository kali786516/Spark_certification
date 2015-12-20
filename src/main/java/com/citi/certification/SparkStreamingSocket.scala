package com.citi.certification

/**
 * Created by kalit_000 on 20/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object SparkStreamingSocket {

  def main (args: Array[String]) {

     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)

    def createSparkStreamingContext() ={
      val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_app").set("spark.hadoop.validateOutputSpecs", "false")
      val sc=new SparkContext(conf)
      val context = new StreamingContext(sc,Seconds(2))
      context.checkpoint("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files")
      import org.apache.spark.streaming._

     def addLengthToState(values:Seq[Int],state:Option[Int]) ={
        Some(values.sum +state.getOrElse(0))
      }

      val stream=context.socketTextStream("localhost",9999)
      stream.print()
      stream.map(x => (x,x.length)).updateStateByKey(addLengthToState).print()
      context
    }

    val ssc=StreamingContext.getOrCreate("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files",createSparkStreamingContext)


    ssc.start()
    ssc.awaitTermination()
  }
}
