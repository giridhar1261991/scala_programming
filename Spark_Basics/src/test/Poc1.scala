package test
import org.apache.spark.sql.SparkSession
import org.apache.log4j._


object Poc1 {
 
  case class users(userid:String, name:String , state:String)
  case class tweets(tweetid:String,tweet:String,userid:String)
  
  def splitUsers(line:String) :users =
  {
    val fields = line.split(',')
    val users_rec:users = users(fields(0),fields(1),fields(2))
    return users_rec    
  }
  
  def splitTweets(line:String) :tweets =
  {
    val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    val tweets_rec:tweets = tweets(fields(0),fields(1),fields(2))
    return tweets_rec    
  }
  
  def main (args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark= SparkSession.builder
    .appName("POC1")
    .master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp")
    .getOrCreate()
    
    
    // val new_data_set = spark.read.csv("D:/Training_doc/Module-5/Module-5/Spark/Dataset/users.csv")
    // new_data_set.show()
    val users_data = spark.sparkContext.textFile("D:/Training_doc/Module-5/Module-5/Spark/Dataset/users.csv")
    val users_record =users_data.map(splitUsers)
    
    val tweets_data = spark.sparkContext.textFile("D:/Training_doc/Module-5/Module-5/Spark/Dataset/tweets.csv")
    val tweets_record =tweets_data.map(splitTweets)
    
    
    import spark.implicits._
    val users_DS = users_record.toDS()
    val tweets_DS = tweets_record.toDS()
    
    users_DS.createOrReplaceTempView("Users_Data")
    tweets_DS.createOrReplaceTempView("Tweets_Data")
    
    val A1=spark.sql("select * from Users_Data where state ='NY'")
    val A2=spark.sql("select * from Tweets_Data where instr(upper(tweet),upper('favorite')) > 0")
    val A3=spark.sql("select userid,count(tweetid) from Tweets_Data group by userid")
    val A4=spark.sql("select userid,count(tweetid) count from Tweets_Data group by userid having count >= 2")
    val A5=spark.sql("select userid,count(tweetid) count from Tweets_Data group by userid having count = 0")
    val A6=spark.sql("select Users_Data.name,count(tweetid) count from Tweets_Data inner join Users_Data on  Tweets_Data.userid= Users_Data.userid group by Users_Data.name")
    val A7=spark.sql("select Users_Data.name,count(tweetid) count from Tweets_Data inner join Users_Data on  Tweets_Data.userid= Users_Data.userid group by Users_Data.name order by count desc")
    val A8=spark.sql("select count(*) from Tweets_Data inner join Users_Data on  Tweets_Data.userid= Users_Data.userid")   
    A8.show()
    //where instr(upper(tweet),upper('favorite')) >0
    //tweets_DS.show()
    
    //val out1 = users_record.collect()
    //out1.foreach(println)
    
    spark.stop()
  }
  
}