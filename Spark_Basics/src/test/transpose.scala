package test
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.log4j._

import org.apache.spark.sql.SparkSession

object transpose {
  
  def main (args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
        .appName("Pivot data")
        .master("local[*]")
        .config("spark.sql.Warehouse.dir","file:///c:/temp")
        .getOrCreate()
     
     val csv_data = spark.read.option("header", "true").csv("D:/Training_doc/Module-5/Module-5/emp_data.txt")
     val data = csv_data.groupBy("Month").pivot("Year").agg(avg("Temp")).orderBy("Month")
     //val A1 = spark.sql("SELECT * FROM high_temps")
     //A1.show()
     data.show()
    
  }
}