package com.citi.certification

/**
 * Created by kalit_000 on 19/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import  org.apache.spark.graphx._
import  org.apache.spark.rdd.RDD

object SparkGraphFromFile {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkGraphFromFile").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val edgelistpath="C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 7\\edges.data"
    val graph: Graph[Int,Int] = GraphLoader.edgeListFile(sc,edgelistpath)

    graph.triplets
    .map { triplets =>
       s"${triplets.srcAttr} ${triplets.attr} ${triplets.dstAttr}"
    }.foreach(println)


    graph.pageRank(0.0001).triplets.map { triplet =>
    s"${triplet.srcAttr} ${triplet.attr} ${triplet.dstAttr}"
    }.foreach(println)

  }

}
