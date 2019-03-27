package test
import org.apache.spark.sql.SparkSession

object SparkSessionBasics {

  case class Users (userid:Int, username:String, age:Int, numfriends:Int)
  

  def splitFields(line:String) : Users =
  {
    val fields = line.split(',')
    val records:Users = Users(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
    return records
  }
  
  def main(args : Array[String])
  {
    val spark = SparkSession.builder
    .appName("abc")
    .master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp")
    .getOrCreate()
    
    val data =spark.sparkContext.textFile("D:/Training_doc/Module-5/Module-5/Spark/SparkSql/fakefriends.csv")
    val records =data.map(splitFields)
    
    import spark.implicits._
    val testDS = records.toDS()
    //testDS.printSchema()
    //testDS.show()
    testDS.createOrReplaceTempView("view_users")
    val test = spark.sql("select count(userid),age from view_users group by age order by age desc")
    val test1 = test.collect()
    test1.foreach(println)
    spark.stop()
  }
}