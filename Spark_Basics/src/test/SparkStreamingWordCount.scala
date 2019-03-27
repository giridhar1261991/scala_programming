package test
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object SparkStreamingWordCount {
  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val lines = ssc.socketTextStream(args(0), args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1))
    //val wc =wordCounts.reduceByKey((x,y) => x+y)
    val wc= wordCounts.reduceByKeyAndWindow((x:Int,y:Int) => (x+y), Seconds(30),Seconds(10))
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}