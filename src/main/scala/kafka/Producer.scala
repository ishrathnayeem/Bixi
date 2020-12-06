package kafka

import java.io.FileReader
import java.util.{Collections, Properties}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import scala.util.Try

object Producer extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class TripData(start_date: String, start_station_code: String, end_date: String,
                      end_station_code: String, duration_sec: String, is_member: String)

  val config = "Resource/CloudConfig.config"
  val topic = "bdsf1901_ishrath_trip"
  val mapper = new ObjectMapper
  val properti: Properties = buildProperties(config)
  val tripData = "Resource/trip.csv"
  val tripDataSchema: StructType = Encoders.product[TripData].schema
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Bixi_Project")
    .getOrCreate()
  val tripInformation: DataFrame = spark.read.option("inferSchema", "false").schema(tripDataSchema)
    .option("header", "true").csv(tripData)

  createTopic(topic, 1, 1, properti)
  val producer = new KafkaProducer[String, JsonNode](properti)
  sendData(tripInformation, topic)

  val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Option(exception) match {
        case Some(err) => println(s"Failed to produce: $err")
        case None => println(s"Produced record at $metadata")
      }
    }
  }

  def sendData(data: org.apache.spark.sql.DataFrame, topic: String): Unit = {
    val mydata = data.select("start_date", "start_station_code", "end_date", "end_station_code",
      "duration_sec", "is_member").toJavaRDD

    for (x <- mydata) {
      val tripRecord = new TripStorage(Option(x.get(0).toString), Option(x.get(1)), Option(x.get(2).toString),
        Option(x.get(3)), Option(x.get(4)), Option(x.get(5)))
      val key: String = "trip-key"
      val value: JsonNode = mapper.valueToTree(tripRecord)
      val record = new ProducerRecord[String, JsonNode](topic, key, value)
      producer.send(record, callback)
    }
  }

  producer.flush()

  def buildProperties(config: String): Properties = {
    val properties: Properties = new Properties
    properties.load(new FileReader(config))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")
    properties
  }

  def createTopic(topic: String, partitions: Int, replication: Int, cloudConfig: Properties): Unit = {
    val newTopic = new NewTopic(topic, partitions, replication.toShort)
    val adminClient = AdminClient.create(cloudConfig)
    Try(adminClient.createTopics(Collections.singletonList(newTopic)).all.get).recover {
      case e: Exception =>
        if (!e.getCause.isInstanceOf[TopicExistsException]) throw new RuntimeException(e)
    }
    adminClient.close()
  }
  println("Produced to topic")
}
