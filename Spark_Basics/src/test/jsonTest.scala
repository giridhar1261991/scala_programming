package test
import org.apache.spark._
import org.apache.spark.sql._



object jsonTest {

  def main (args : Array[String])
  {
    val conf = new SparkConf().setMaster("local").setAppName("Json parser")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val json_data = sqlContext.read.json("D:/test_json.json")
    val flattenJSON = json_data.select("paragraphs.title","paragraphs.user","paragraphs.dateUpdated","paragraphs.config.colWidth")
    flattenJSON.show(10)
  }
  
}