package test
import org.apache.spark.SparkContext
import org.apache.log4j._

object task1 {
  
  def main(args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[1]","Task_1")
    val input = sc.textFile("D:/Training_doc/Spark/Spark/practicals/Datasets/babynames.csv")
    val t1 = input.filter(f => f.startsWith("A") || f.startsWith("K"))
    val t2 = t1.take(10)
    t2.foreach(println)
  }  
  
}