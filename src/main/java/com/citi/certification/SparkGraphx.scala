package com.citi.certification

/**
 * Created by kalit_000 on 19/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import  org.apache.spark.graphx._
import  org.apache.spark.rdd.RDD

object SparkGraphx {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkGraph").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val vertices = sc.parallelize(Array(
      (1L, "John"),
      (2L, "kali"),
      (3L, "Apeksha"),
      (4l, "Ajeeth")
    ))

    val edges = sc.parallelize(Array(
      Edge(1L, 2L, "Loves"),
      Edge(1L, 2L, "Hates"),
      Edge(2L, 3L, "Loves"),
      Edge(3L, 2L, "Loves")
    ))

    val graph = Graph(vertices, edges)

    graph.subgraph(
      epred =(triplet) => triplet.attr == "Loves",
    )
      .triplets.map { triplet =>
      s"${triplet.srcAttr} ${triplet.attr} ${triplet.dstAttr}"
    }.foreach(println)

    val nedges=graph.edges.filter(_.attr == "Loves").count()

    println(s"$nedges Love")

  }
}
