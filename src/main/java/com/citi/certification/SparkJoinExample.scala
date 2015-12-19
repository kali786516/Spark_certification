package com.citi.certification

/**
 * Created by kalit_000 on 12/12/2015.
 */

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.springframework.context.support.ClassPathXmlApplicationContext

case class Register(uuid:String,date:String,customerId:Int,lat:Double,long:Double)

case class Click(uuid:String,date:String,pageId:Int)

object SparkJoinExample {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkJoinExample").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

   val regfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 2\\join\\reg.tsv")
                           .map(x => x.split("\t")).map(x=> Register(x(1).toString,x(0),x(2).toInt,x(3).toDouble,x(4).toDouble)).map(r => (r.uuid,r))

    println(regfile.first()._2)

   val clicks=sc.textFile("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 2\\join\\clk.tsv")
                          .map(x => x.split("\t")).map(x => Click(x(1),x(0),x(2).toInt)).map(y => (y.uuid,y))
     //map(x => (x(1),Click(x(1),x(0),x(2).toInt)))
   println(clicks.first()._2)

    val joined=clicks.join(regfile)

    joined.foreach(println)



  }
}
