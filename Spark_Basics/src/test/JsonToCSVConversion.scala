package test
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object JsonToCSVConversion {
  def main(args : Array[String]){
      Logger.getLogger("org").setLevel(Level.OFF)
      
      val spark = SparkSession
                  .builder()
                  .appName("JsonToCSVConversion")
                  .master("local[*]")
                  .config("spark.sql.warehouse.dir", "file:///tmp")
                  .getOrCreate()
      
      val jsonFile = spark.read.json("D:/Training_doc/Module-5/Module-5/Spark/customers.json")
      val flattenJSON = jsonFile.select("custno", "firstname","lastname","gender","age","profession","contactNo","emailId","city","state","isActive","createdDate","UpdatedDate")
      flattenJSON.show(4)
      flattenJSON.write.mode(SaveMode.Overwrite).csv("D:/Training_doc/Module-5/Module-5/Spark/lz")
      println("Process completed.....")
      
  }
}

