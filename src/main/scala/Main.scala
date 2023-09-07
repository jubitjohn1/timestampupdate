
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
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.io.Source


object Main  {

  case class ConfigData(cei_code: String, PK: String)
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args:Array[String]): Unit={
    val spark= SparkSession.builder
    .master("local")
    .appName("TimeStampProject")
    .getOrCreate()


  spark.conf.set("spark.sql.session.timeZone", "UTC")
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

 
  val Filepath_config: String = args(0)
  val epochTimestamp: Long = args(1).toLong
  val OutputFilePath: String = args(2)
  val InputFilePath: String =args(3)


  try {


    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputStream = fs.open(new Path(Filepath_config))
    val jsonString = Source.fromInputStream(inputStream).mkString
    val jsonArray = parse(jsonString).asInstanceOf[JArray]

    import spark.implicits._
    implicit val formats = DefaultFormats

    val configDataList = jsonArray.arr.map { json =>
    val cei_code = (json \ "cei_code").extract[String]
    val PK = (json \ "PK").extract[String]
    ConfigData(cei_code, PK)
    }.toList

    // Extract the cei_code values into a list
    val ceiCodeList = configDataList.map(_.cei_code)
    implicit val configDataEncoder = Encoders.product[ConfigData]
    val df: DataFrame= spark.read.option("header","true").csv(InputFilePath)
    
    df.show()
    val formattedDF = df.withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd-HH:mm:ss"))
    val Final=formattedDF.filter(col("updated_at") <= to_timestamp(lit(epochTimestamp)))
    println("Filtered DF")
    Final.show()

  // Process each cei_code and collect the resulting DataFrames
    ceiCodeList.foreach { ceiCode =>
      val ceiCodeDF = Final.filter(col("cei_code") === ceiCode)
      val matchingConfigDataOption = configDataList.find(_.cei_code == ceiCode)
      //    Extract the PK if a matching ConfigData instance is found
      val selectedPK = matchingConfigDataOption match {
        case Some(configData) => configData.PK 
        case None => ""
}
      val windowSpec = Window.partitionBy(selectedPK).orderBy(col("updated_at").desc)
      val rankedDF121 = ceiCodeDF.withColumn("timeStampRankPK", row_number().over(windowSpec))
      val finalDf= rankedDF121.filter(col("timeStampRankPK") === 1).select(col("cei_code"),col("device_name"),col("user_name"),col("cei_status"),col("updated_at"))

       finalDf.write
      .partitionBy("updated_at","cei_code")
      .mode("overwrite")
      .csv(OutputFilePath)
    }


  }catch{
    case e: Exception =>
    logger.warn("File Not Found", e)
    }

  spark.stop()

  }
}