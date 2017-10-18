import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


// On the console run "nc -lk 9999"


object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    def updateFunction(values : Seq[Int], runningCount : Option[Int]) ={
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    ssc.checkpoint("./tmp_checkpoint")

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    val totalWordCount = wordCounts.updateStateByKey(updateFunction _)
    totalWordCount.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

  }
