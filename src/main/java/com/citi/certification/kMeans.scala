package com.citi.certification

/**
 * Created by kalit_000 on 19/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

object kMeans {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkKMeans").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path="C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 6\\iris.data"

    //vector only takes int or double
    val data : RDD[org.apache.spark.mllib.linalg.Vector]=sc.textFile(path)
    .map(line =>
            Vectors.dense(
              line.split(",").slice(0,3).map(_.toDouble))
      )

    //computed centres
    val computedcnetres= KMeans.train(data,k=10,maxIterations = 200)
    val wsse=computedcnetres.computeCost(data)

    //error should be less
    println(s"With in set of error (error should be less , increase number of clusters even though it doesnt mean the algorithm is good) :-${wsse}")

  }

}
