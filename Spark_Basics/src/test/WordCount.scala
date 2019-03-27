package test
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCount {
  var a=10 // Variable mutable
  val b=20 //immutable
  
  def main (args : Array[String])
  {
     Logger.getLogger("org").setLevel(Level.OFF)
     val sc = new SparkContext("local[2]","applicationName")
     val data =sc.textFile("D:/Training_doc/Module-5/Module-5/Spark/WordCount/sample.txt")
     val words =data.flatMap(line => line.split(','))
     val wordCounts = words.countByValue()
     //val records = words.collect()
     //val records = wordCounts.collect()
     wordCounts.foreach(println)
     
  }
}