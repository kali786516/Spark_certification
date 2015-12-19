package com.citi.certification

/**
 * Created by kalit_000 on 19/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

object SparkSVMMLIB {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSVMMLLIB_Titanic").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val  sqlcontext=new SQLContext(sc)

    val path="C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_certification\\Study Guide for the Developer Certification for Apache Spark - Working Files\\Chapter 5\\data_titanic.json"
    val df=sqlcontext.load(path,"json")
    df.printSchema()

    val persons=df.select("name","age","sex","survived","parch").rdd

    val features=persons.map(row =>
    LabeledPoint(row.getLong(3).toDouble,
    Vectors.dense(
    row.getDouble(1),row.getLong(4),if(row.getString(2) == "M") 0 else 1

      )
     )
    )

    val split=features.randomSplit(Array(0.6,0.4))
    val trainingset=split(0)
    val validatationset=split(1)


    val model=SVMWithSGD.train(trainingset,50)

    val scoreandLabels=validatationset.map {points =>
    val score=model.predict(points.features)
      (score,points.label)
    }

    val metrics=new BinaryClassificationMetrics(scoreandLabels)
    val auroc=metrics.areaUnderROC()

    println("Area under Roc=" + auroc)


  }


}
