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


object Test {

  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkCertificationTest").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    println("Hi kali")

  }

}
