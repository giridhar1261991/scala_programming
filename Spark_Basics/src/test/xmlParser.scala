package test
import org.apache.spark._
import org.apache.spark.sql._
import com.univocity.parsers.common.Format
import org.apache.spark.sql.functions.explode

object xmlParser {

  def main (args: Array[String])
  {
    val spark = SparkSession.builder()
    .master("local[*]")
    .appName("XLMread")
    .config("spark.sql.Warehouse.dir","file:///c:/temp")
    .getOrCreate()
    
    import spark.implicits._
    val xmlData= spark.read.format("com.databricks.spark.xml").option("rowTag", "product").load("D:/Training_doc/Spark/xmlParse.xml")
    val flatten=xmlData.withColumn("catalogitem", explode($"catalog_item")).withColumn("sizedetails", explode($"catalogitem.size")).withColumn("sizecolor", explode($"sizedetails.color_swatch"))
    flatten.printSchema()
    val a1= flatten.createOrReplaceTempView("xml_data")
    spark.sql("select _description Description,_product_image image,catalogitem._gender gender,catalogitem.price,sizedetails._description size_description,sizecolor._VALUE color from xml_data limit 10").show(10,false)
    
    
    
  }
}