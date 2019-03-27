package test
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import java.io.File._

object HashChar 
{
  
  def main (args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder
    .appName("HashChar")
    .master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp")
    .getOrCreate()
        
    def data = spark.sparkContext.textFile("D:/Training_doc/Module-5/Module-5/Spark/Dataset/hash_wc_dataset.txt")
    import spark.implicits._
    def data_records = data.flatMap(line => line.split(" ").filter(p => p.startsWith("#"))).toDS()
    val out = data_records.show()
  }
  
}