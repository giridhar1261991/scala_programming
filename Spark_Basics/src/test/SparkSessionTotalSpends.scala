package test
import org.apache.spark.sql.SparkSession

object SparkSessionTotalSpends {

  case class Users (Customer_id:Int, OrderId:Int, Expence:Float)

  def splitFields(line:String) : Users =
  {
    val fields = line.split(',')
    val records:Users = Users(fields(0).toInt,fields(1).toInt,fields(2).toFloat)
    return records
  }
  
  def main(args : Array[String])
  {
    val spark = SparkSession.builder
    .appName("abc")
    .master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp")
    .getOrCreate()
    
    val data =spark.sparkContext.textFile("D:/Training_doc/Module-5/Module-5/Spark/TotalSpentByCustomer/customer-orders.csv")
    val records =data.map(splitFields)
    
    import spark.implicits._
    val testDS = records.toDS()
    //testDS.printSchema()
    //testDS.show()
    testDS.createOrReplaceTempView("view_users")
    val test = spark.sql("select Customer_id,sum(Expence) Expence from view_users group by Customer_id")
    val test1 = test.collect()
    test1.foreach(println)
    spark.stop()
  }
}