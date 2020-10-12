package convertion

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

case class StationInformationData
(station_id:String,external_id:String,name:String,short_name:String,lat:String, lon:String,rental_methods:Array[String],
 capacity:String, electric_bike_surcharge_waiver:String,is_charging:String,eightd_has_key_dispenser:String,
 has_kiosk:String)
case class Data(stations:Array[StationInformationData])
case class StationInformation(last_updated: String,ttl:String,data:Data)

case class SystemInformationData
(system_id:String,language:String,name:String,short_name:String,operator:String,url:String,purchase_url:String,
 start_date:String,phone_number:String,email:String,license_url:String,timezone:String)
case class SystemInformation(last_updated:String,ttl:String,data:SystemInformationData)

class JsonToCsv {

  val stationInformationSchema: StructType = Encoders.product[StationInformation].schema
  val systemInformationSchema: StructType = Encoders.product[SystemInformation].schema

  val stationInformation:
    String = "/Users/ishrathnayeem/MY MAC/Study/Big Data/MCIT/Final Project/JSON/station_info.json"
  val systemInformation:
    String = "/Users/ishrathnayeem/MY MAC/Study/Big Data/MCIT/Final Project/JSON/system_info.json"

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("JSONtoCSV").getOrCreate()

  val stationInformationData: DataFrame = spark.read.option("multiline", "true").option("mode", "PERMISSIVE")
    .json(stationInformation)
  val systemInformationData: DataFrame = spark.read.option("multiline", "true").option("mode", "PERMISSIVE")
    .json(systemInformation)

  val stationInformationDataFetch: DataFrame = stationInformationData.select(col("data")
    .getField("stations").as("stations"))

  val stationData: DataFrame =
    stationInformationDataFetch.withColumn("stations", explode(col("stations"))).select(
      col("stations").getField("station_id").as("station_id"),
      col("stations").getField("external_id").as("external_id"),
      col("stations").getField("name").as("station_name"),
      col("stations").getField("short_name").as("short_name"),
      col("stations").getField("lat").as("lat"),
      col("stations").getField("lon").as("lon"),
      col("stations").getField("rental_methods").getItem(0).as("rental_methods"),
      col("stations").getField("rental_methods").getItem(1).as("rental_methods_1"),
      col("stations").getField("capacity").as("capacity"),
      col("stations").getField("electric_bike_surcharge_waiver")
        .as("electric_bike_surcharge_waiver"),
      col("stations").getField("is_charging").as("is_charging"),
      col("stations").getField("eightd_has_key_dispenser").as("eightd_has_key_dispenser"),
      col("stations").getField("has_kiosk").as("has_kiosk")
    )

  val systemData: DataFrame = systemInformationData.select(
    col("last_updated").as("last_updated"),
    col("ttl").as("ttl"),
    col("data").getField("system_id").as("system_id"),
    col("data").getField("language").as("language"),
    col("data").getField("name").as("name"),
    col("data").getField("operator").as("operator"),
    col("data").getField("url").as("url"),
    col("data").getField("purchase_url").as("purchase_url"),
    col("data").getField("start_date").as("start_date"),
    col("data").getField("phone_number").as("phone_number"),
    col("data").getField("email").as("email"),
    col("data").getField("license_url").as("license_url"),
    col("data").getField("timezone").as("timezone")
  )

  val csvFile: DataFrame = stationData.crossJoin(systemData)
  csvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("hdfs://quickstart.cloudera/user/fall2019/ishrath/enriched_station_system")

}