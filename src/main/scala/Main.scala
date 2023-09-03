
import org.apache.spark.sql.{SparkSession,DataFrame,Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders


object Main  {

  case class ConfigData(cei_code: String, PK: String)

  def main(args:Array[String]): Unit={
  val spark= SparkSession.builder
  .master("local")
  .appName("TimeStampProject")
  .getOrCreate()

  println("Testing")

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val Filepath_config: String = args(0)
  val epochTimestamp: Long = args(1).toLong
  val OutputFilePath: String = args(2)
  val InputFilePath: String =args(3)


  implicit val configDataEncoder = Encoders.product[ConfigData]
  import spark.implicits._
  val config: Dataset[ConfigData] = spark.read.option("multiline","true").json(Filepath_config).as[ConfigData]


  val nameList: List[String] = config.map(configData => configData.cei_code).collect().toList

// Print the cei_code values
  println("Testing List")
  nameList.foreach(name => println(name))
  



  val inputEpoch = spark.sql(s"SELECT to_timestamp(from_unixtime($epochTimestamp), 'yyyy-MM-dd HH:mm:ss') as formatted_date")

  val df: DataFrame= spark.read.option("header","true").csv(InputFilePath)
  df.show()
  val formattedDF = df.withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd-HH:mm:ss"))

  val formattedTimestamp = inputEpoch.select("formatted_date").head().getAs[java.sql.Timestamp](0)

  val windowSpec = Window.partitionBy("cei_code").orderBy(col("updated_at").desc)

  val rankedDF = formattedDF.withColumn("timeStampRank", rank().over(windowSpec))

  val Final=rankedDF.filter(col("updated_at") <= formattedTimestamp)

  println("Filtered DF")

  Final.show()

  // val distinctCeiCodes = config.select("cei_code").distinct().collect()

  // Process each cei_code and collect the resulting DataFrames
    val resultDataFrames = nameList.flatMap { ceiCode =>
      // val ceiCode = ceiCodeRow.getString(0)
      val ceiCodeDF = Final.filter(col("cei_code") === ceiCode)
      val selectedPK = config.filter(col("cei_code") === ceiCode).select("PK").head().getString(0)
      val windowSpec = Window.partitionBy(selectedPK).orderBy(col("updated_at").desc)
      val rankedDF121 = ceiCodeDF.withColumn("timeStampRankPK", rank().over(windowSpec))
      Some(rankedDF121.filter(col("timeStampRankPK") === 1))
    }

    // Union all the resulting DataFrames
    val finalResult = resultDataFrames.reduceOption(_ union _).getOrElse(spark.emptyDataFrame)


    finalResult.write.mode("overwrite").csv(OutputFilePath)

    // Show the final result
    finalResult.show()
    val finalDf= finalResult.select(col("cei_code"),col("device_name"),col("user_name"),col("cei_status"),col("updated_at"))
    finalDf.show()


  spark.stop()

  
  }
}