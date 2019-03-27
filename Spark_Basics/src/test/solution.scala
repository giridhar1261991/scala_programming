package test
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object solution 
{
  
  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.DEBUG)
    val spark = SparkSession.builder()
        .master("local[*]")
        .appName("Sample")
        .config("spark.sql.Warehouse.dir","file:///c:/temp")
        .getOrCreate()
        spark.sparkContext.setLogLevel("DEBUG")
    //Json
    val json_data = spark.read.json("D:/Training_doc/Project/3_6_Feb_2019/3_6_Feb_2019/JSON_s1/transactions.json")
    // println(json_data.show(5))
    //CSV
    val csv_data = spark.read.csv("D:/sample_data.csv")
    // println(csv_data.show(5))
    val csv_data_header = spark.read.format("com.databricks.spark.csv").option("header","true").load("D:/sample_data.csv")
    println(csv_data_header.show(5))
    //xml
    
    
  }
  
}