package test
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object JSONParser {

  def main (args :Array[String])
  {
     val spark = SparkSession
                  .builder()
                  .appName("JsonToCSVConversion")
                  .master("local[*]")
                  .config("spark.sql.warehouse.dir", "file:///tmp")
                  .getOrCreate()
       val inputPath= args(0)
       val jsonFile= spark.read.json(inputPath)
       val flattenJSON = jsonFile.select("info.custno", "firstname","lastname","gender","age","profession","contactNo","emailId","city","state","isActive","createdDate","UpdatedDate")
       flattenJSON.show(4)
  }
  
  
  
}