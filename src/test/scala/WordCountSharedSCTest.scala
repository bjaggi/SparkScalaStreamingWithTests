import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class WordCountSharedSCTest extends FunSuite with SharedSparkContext {
  
  private val wordCount = new WordCount

  test("get & check word count RDD"){
    val result = wordCount.countWords("",sc)
    println("Data From WordCount !")
    println(result.first())

    assert(result.take(2).length == 2)
    Assert.assertEquals(result.first(),("got",1))
  }



  
}