import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.Assert


class WordCountTest extends FunSuite with BeforeAndAfterAll {
  
  private val wordCount = new WordCount
  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  //private val checkpointDir = Files.createTempDirectory(appName).toString

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName).set("spark.driver.allowMultipleContexts", "true")

    sc = new SparkContext(conf)

    ssc = new StreamingContext(conf, batchDuration)
    val linesDStream = ssc.textFileStream("/user/user01/stream")

    //ssc.checkpoint(checkpointDir)
    //sc = ssc.sparkContext
  }


  test("get & check word count RDD"){
    val result = wordCount.countWords("",sc)
    println("Data From WordCount !")
    println(result.first())

    assert(result.take(2).length == 2)
    Assert.assertEquals(result.first(),("got",1))
  }


  override def afterAll(): Unit = {
    sc.stop()
  }

  
}