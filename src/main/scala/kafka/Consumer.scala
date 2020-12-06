package kafka

import java.io.FileReader
import java.util.{Collections, Properties}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

object Consumer extends App with Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new Configuration()
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/hdfs-site.xml"))
  val hadoop = FileSystem.get(conf)

  val path = new Path("/user/fall2019/ishrath/final")

    if (hadoop.exists(path))
      try {
        hadoop.delete(path, true)
        hadoop.mkdirs(path)
      }

  case class Trip(start_date: Option[String], start_station_code: Option[String], end_date: Option[String],
                  end_station_code: Option[String], duration_sec: Option[String], is_member: Option[String])

  def parseTrip(str: String): Trip = {
    val regex = """"((?:[^"\\]|\\[\\"ntbrf])+)"""".r
    val str1 = regex.findFirstMatchIn(str).get.group(1)
    val p = str1.split(",")
    println(p(0) + " " + p(1) + " " + p(2) + " " + p(3))
    Trip(Option(p(0)), Option(p(1)), Option(p(2)), Option(p(3)), Option(p(4)), Option(p(5)))
  }

  val sparkConf = new SparkConf().setAppName("Bixi_Project")
  val spark = SparkSession.builder().master("local[*]").appName("Bixi_Project").getOrCreate()
  val sc = new StreamingContext(spark.sparkContext, Seconds(1))
  val enrichStationFile = "hdfs://quickstart.cloudera/user/fall2019/ishrath/enriched_station_system"
  val enrichStationDataframe: DataFrame = spark.read.option("inferSchema", "false")
    .option("header", "true").csv(enrichStationFile).select( "system_id", "timezone", "station_id", "name", "lat", "lon", "capacity", "short_name")

  import spark.implicits._

  val config = "Resource/CloudConfig.config"
  val topic = "bdsf1901_ishrath_trip"
  val properti = buildProperties(config)
  val consumer = new KafkaConsumer[String, JsonNode](properti)
  val mapper = new ObjectMapper

  consumer.subscribe(Collections.singletonList(topic))
  while (true) {
    val records = consumer.poll(100)

    for (record <- records) {
      val key = record.key()
      val value = record.value().toSeq
      val rdd = spark.sparkContext.parallelize(value)
      val df = rdd.map(x => parseTrip(x.toString)).toDF()

    val enrichtrip = df.join(enrichStationDataframe, $"short_name" === $"start_station_code", "inner")
      enrichtrip.write.format("csv").option("header", "True").mode("append").option("sep", ",")
        .save("hdfs://quickstart.cloudera/user/fall2019/ishrath/final")
    }
  }
  consumer.close()

  def buildProperties(config: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Bixi_Project")
    properties.load(new FileReader(config))
    properties
  }
  println("Consumer Running")
}

