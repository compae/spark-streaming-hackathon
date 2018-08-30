package com.spark.hackathon.streaming.explicacion

import akka.event.slf4j.SLF4JLogging
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._


/**
  * Repasar las funcionalidades básicas de Streaming de Spark
  */
object SparkStreamingMain extends App with SLF4JLogging {


  /** Creación de la session */

  val configurationProps = ConfigFactory.load().getConfig("spark").entrySet()
    .map(prop => (s"spark.${prop.getKey}", prop.getValue.unwrapped().toString)).toSeq
  val sparkConf = new SparkConf().setAll(configurationProps)
  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

  import sparkSession.implicits._


  /** Creación de streams sobre kafka */

  val tiendas = sparkSession.read
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/home/jcgarcia/hackathon/tiendas.csv")

  tiendas.createOrReplaceTempView("tiendas")
  val jsonSchema = sparkSession.read.json("/home/jcgarcia/hackathon/tickets.json").schema

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "uax",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Seq("tickets")
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  val keyValueStream = stream.map(record => (record.key, record.value))
  val tickets = keyValueStream.map(tuple => Row(tuple._2))

  tickets.foreachRDD { rdd =>

    val ticketsDf = sparkSession.createDataFrame(rdd, StructType(Seq(StructField("ticket", StringType))))

    ticketsDf.withColumn("ticket2", from_json($"ticket", jsonSchema)).createOrReplaceTempView("tickets")

    val joinBatchStreaming = sparkSession.sql(
      "SELECT tickets.ticket2.storeId, tiendas.nombre " +
        "FROM tickets " +
        "JOIN tiendas " +
        "ON tickets.ticket2.storeId = tiendas.id"
    )

    joinBatchStreaming.printSchema()
    joinBatchStreaming.show(100)
  }

  streamingContext.start()
  streamingContext.awaitTerminationOrTimeout(1000000)

}

