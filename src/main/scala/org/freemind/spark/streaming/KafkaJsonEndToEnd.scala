package org.freemind.spark.streaming

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object KafkaJsonEndToEnd {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: KafkaJsonEndToEnd <output-sink>")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("KafkaJsonEndToEnd").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic_json")
      .load()

    def outputToConsole(): Option[StreamingQuery] = {
      println("Output to console")
      val query = inputDf.withColumn("zip", get_json_object($"value".cast("string"), "$.zip")).
        groupBy(window($"timestamp", "10 seconds", "5 seconds"), $"zip").count().
        writeStream.
        format("console").
        option("truncate", "false").
        option("numRows", 25).
        trigger(Trigger.ProcessingTime("25 seconds")).
        start()
      Some(query)
    }

    def outputToParquet(): Option[StreamingQuery] = {
      println("Output to console")
      val selectDf = inputDf.select($"value".cast("string"))
          .select(get_json_object($"value", "$.zip").as("zip"),
            get_json_object($"value", "$.hittime").as("hittime"),
            date_format(get_json_object($"value", "$.hittime"), "yyyy-MM-dd").as("day")
          )

      val query = selectDf.writeStream.
        format("parquet").
        option("path", "sample/test-data").
        option("checkpointLocation", "sample/check").
        partitionBy("zip", "day").
        trigger(Trigger.ProcessingTime("25 seconds")).
        start()

      Some(query)
    }

    if (args.length < 1) {
      System.err.println("Usage: KafkaJsonEndToEnd <output-sink> [<join>]")
      System.exit(-1)
    }

    val sink = args(0)
    val query: Option[StreamingQuery] = sink match {
      case "console" => outputToConsole()

      case "parquet" => outputToParquet()
      case _ => None
    }












  }


}
