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
import org.apache.spark.storage.StorageLevel

object PersistCache {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("PersistCache").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val tagsfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 4\\movielens\\large\\tags.dat")
                            .map(x => x.split("::"))

    tagsfile.first().foreach(println)

    println(tagsfile.filter(x =>x.contains("15")).count())

    val moive15=tagsfile.filter(x => x.contains("15"))

    //moive15.cache()

    moive15.persist(StorageLevel.MEMORY_AND_DISK)



  }

}
