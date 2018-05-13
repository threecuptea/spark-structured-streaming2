package org.freemind.spark.streaming

import java.sql.Timestamp

import org.apache.spark.sql
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/**
  * https://docs.databricks.com/spark/latest/structured-streaming/examples.html#stream-stream-joins
  *
  * https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html
  *
  * https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
  *
  * Generates data at the specified number of rows per second, each output row contains a timestamp and value.
  * Where timestamp is a Timestamp type containing the time of message dispatch, and value is of Long type
  * containing the message count, starting from 0 as the first row. This source is intended for testing and benchmarking.
  * As of Spark 2.3, you can use joins only when the query is in Append output mode.
  * Other output modes are not yet supported.
  *
  * Stream-stream left-outer join must specify watermark on the right and time constraints for correct results.
  * Without watermark on the right, will keep waiting and unable to generate NULL result.
  *
  * The outer NULL results will be generated with a delay that depends on the specified watermark delay and the time
  * range condition. This is because the engine has to wait for that long to ensure there were no matches and
  * there will be no more matches in future.
  *
  * In the current implementation in the micro-batch engine, watermarks are advanced at the end of a micro-batch,
  * and the next micro-batch uses the updated watermark to clean up state and output outer results. Since we trigger
  * a micro-batch only when there is new data to be processed, the generation of the outer result may get delayed
  * if there no new data being received in the stream. In short, if any of the two input streams being joined
  * does not receive data for a while, the outer (both cases, left or right) output may get delayed.
  *
  * @author sling/ threecuptea
  */
object RateStreamStreamJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("RateStreamStreamJoin").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    def outputToConsole(theJoin: sql.DataFrame): Option[StreamingQuery] = {
      println("Output to console")
      val query = theJoin.writeStream.
        format("console").
        option("truncate", "false").
        option("numRows", 50).
        trigger(Trigger.ProcessingTime("25 seconds")).
        start()
      Some(query)
    }

    def outputToMemory(theJoin: sql.DataFrame): Option[StreamingQuery] = {
      println("Output to memory")
      val query = theJoin.writeStream.
        format("memory").
        queryName("impressionClick").
        trigger(Trigger.ProcessingTime("25 seconds")).
        start()
      //Need some sleep to get something processed.   console output always sync with trigger
      // query.lastProgress is the json I got using console.
      Thread.sleep(26000)
      //This will have full result since query directly
      spark.sql("select * from impressionClick").show(50, false)

      Thread.sleep(25000)
      spark.sql("select * from impressionClick").show(50, false)

      Thread.sleep(25000)
      spark.sql("select * from impressionClick").show(50, false)

      Some(query)
    }

    def outputToParquet(theJoin: sql.DataFrame): Option[StreamingQuery] = {
      println("Output to Parquet")
      val query = theJoin.writeStream.
        format("parquet").
        option("path", "impression_click/data").
        option("checkpointLocation", "impression_click/check").
        //partitionBy(); For kafka example, I might be able to try it by resource_type and date
        trigger(Trigger.ProcessingTime("25 seconds")).
        start()

      Thread.sleep(45000)
      spark.read.parquet("impression_click/data").show(50, false)

      Some(query)
    }

    def outputToMysql(theJoin: sql.DataFrame): Option[StreamingQuery] = {
      println("Output to mysql")
      //Need to create a
      val url="jdbc:mysql://localhost:3306/danube"
      val user ="danube"
      val pwd = "temp123"
      val table = "impression_click"

      val writer = new JDBCSink(url,user, pwd, table)
      val query = theJoin.as[(String, String, String, String)]
        .writeStream
        .foreach(writer)
        .trigger(Trigger.ProcessingTime("25 seconds"))
        .start()
      Some(query)
    }

    if (args.length < 1) {
      System.err.println("Usage: RateStreamStreamJoin <output-sink> [<join>]")
      System.exit(-1)
    }

    val sink = args(0)
    val impressions = spark.readStream.format("rate").option("rowsPerSecond", 5).option("numPartitions", 1).load().
      select($"value".as("adId"), $"timestamp".as("impressionTime"))


    val clicks = spark.readStream.format("rate").option("rowsPerSecond", 5).option("numPartitions", 1).load().
      where((rand() * 100).cast("integer") < 10).
      select(($"value" - 40).as("adId"), $"timestamp".as("clickTime")).
      where($"adId" > 0)  // 40 / 5 = 8 seconds, 80/5 = 16

    val impressionsWithWatermark = impressions.select($"adId".as("impressionAdId"), $"impressionTime").
      withWatermark("impressionTime", "10 seconds")  //It's safe to use 10.  10 < 16, you will drop something you should not drop
    //That's why there is gap there.
    val clicksWithWatermark = clicks.select($"adId".as("clickAdId"), $"clickTime").
      withWatermark("clickTime", "10 seconds")

    val innerJoin = impressionsWithWatermark.join(clicksWithWatermark, expr(
      """
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <=  impressionTime + interval 10 seconds
      """))

    val leftOuter = impressionsWithWatermark.join(clicksWithWatermark, expr(
      """
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <=  impressionTime + interval 10 seconds
      """), joinType = "leftOuter")

    val theJoin = if (args.length > 1) leftOuter else innerJoin

    val query: Option[StreamingQuery] = sink match {
      case "console" => outputToConsole(theJoin)
      case "memory" => outputToMemory(theJoin)
      case "parquet" => outputToParquet(theJoin)
      case "mysql" => outputToMysql(theJoin)
      case _ => None
    }

    if (query.isDefined)
      query.get.awaitTermination()

    /*

    innerJoin.writeStream
      .foreach(new ForeachWriter[Row] {

        override def process(row: Row): Unit = {
          println(s"Processing ${row}")
        }

        override def close(errorOrNull: Throwable): Unit = {}

        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }
      })
      .start()
  */
  }

}
