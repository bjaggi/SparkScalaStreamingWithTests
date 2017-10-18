//package com.eva.app

import org.apache.spark._
import org.apache.spark.rdd.RDD


class WordCountProcessor {
  //methods have been moved to a class to unit test it
  def getWordsByLine(wordsByLine: RDD[String]): RDD[(String)] = {
    wordsByLine.flatMap(line => line.split(" "))

    /*val counts = wordsByLine.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)*/


  }
}


class WordCount {

  System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master\\");


  def countWords(someText: String, sc: SparkContext) : RDD[(String, Int)]= {
    val conf = new SparkConf()
    conf.setAppName("wordCount")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val textFromFile = sc.textFile("/Users/brijeshjaggi/Documents/Softwares_And_Projs/wordcount.txt")
    textFromFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)


  }

    /**
    * Main method is for counting word using spark submit
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordCount")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    //val textFile = sc.textFile("hdfs:///user/cloudera/words.txt")
    val textFile = sc.textFile("/Users/brijeshjaggi/Documents/Softwares_And_Projs/wordcount.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    //counts.saveAsTextFile("hdfs:///user/cloudera/spark_output/wordscount")
    counts.saveAsTextFile("/Users/brijeshjaggi/Documents/Softwares_And_Projs/spark_output")
    println("====> ")
    counts.collect().foreach(println)
  }

}