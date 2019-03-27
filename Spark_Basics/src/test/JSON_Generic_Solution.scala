import org.apache.spark._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream

object JSON_Generic_Solution {

  def main(args: Array[String]): Unit = {
    var prop: Properties = new Properties()
    val inputFile = args(0)
   //val inputFile = "path/products.json"
     val outputPath = args(1)
   // val outputPath = "path/folder"

    val conf = new SparkConf().setMaster("local").setAppName("jsonToCsv")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    try {
      val JsonDataFrame = sqlContext.read.json(inputFile)
      val prop_file = args(2)
      //val prop_file = "path/fields.txt"
      prop.load(new FileInputStream(prop_file))

      val jsonColumnsList = prop.getProperty("customer_fields")
      if(jsonColumnsList == null){
        println("Error: jsonColumnsList not found..")
      }
      println("JSON Columns:"+jsonColumnsList)

      val jsonParse = JsonDataFrame.selectExpr(jsonColumnsList.split(","): _*).repartition(1)
        jsonParse.write.csv(outputPath)
   
    }
    catch{
      case ex: NullPointerException => {
        println("Error: jsonColumnsList  are not populated..")
        System.exit(1)
      }
    }
    println("Execution completed..")
  }

}