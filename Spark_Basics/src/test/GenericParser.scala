package test
import org.apache.spark._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j._

object GenericParser {
  def main (args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var prop: Properties = new Properties()
    val inputpath = args(0)
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val JsonDataFrame = sqlContext.read.json(inputpath)
    val propFile = args(1)
    prop.load(new FileInputStream(propFile))
    val jsonColumnList = prop.getProperty(args(2))
    println("Json Columns:" +jsonColumnList)
    val jsonParse = JsonDataFrame.selectExpr(jsonColumnList.split(","):_*)
    jsonParse.show()    
    
  }
}