package org.freemind.spark.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Start nc as the followings
  * 'nc -lk 9999'
  *
  * $SPARK_HOME/bin/spark-submit -master local[4] --class org.freemind.spark.streaming.WordCountWindowed \
  * build/libs/spark-structured-streaming2-0.0.1-SNAPSHOT.jar localhost 9999 10 5
  *
  * The default output mode is 'Append'.  I will get error if I use 'Append' mode since this is applicable only on the
  * queries where existing rows in the Result Table are not expected to change.   This application does not have
  * watermark. Therefore, the result is always subject to change and in partial aggregate state.  Append will have
  * no trouble if no aggregation is involved.
  *
  * Complete Mode - The entire updated Result Table will be written to the external storage.
  * Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the storage.
  * This is applicable only on the queries where existing rows in the Result Table are not expected to change.
  * Complete Mode - The entire updated Result Table will be written to the external
  * storage. It is up to the storage connector to decide how to handle writing of the entire table.
  *
  * Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the
  * external storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this
  * mode only outputs the rows that have changed since the last trigger.
  * If the query doesn’t contain aggregations, it will be equivalent to Append mode.
  *
  * Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10)
  * but not in [12:00,12:05).
  *
  * Conditions for watermarking to clean aggregation state: Output mode must be Append or Update. Complete mode
  * requires all aggregate data to be preserved, and hence cannot use watermarking to drop intermediate state.
  *
  * Queries with aggregation
  * With watermark: all 3 output modes are supported. In append mode,  rows can be added to the Result Table only once
  * after they are finalized (i.e. after watermark is crossed). Update mode uses watermark to drop old aggregation state.
  * Complete mode does not drop old aggregation state since it is guaranteed to preserve all states.
  * Without watermark:  only complete and update output modes
  *
  * Queries with joins: Only 'append' mode is supported. Output only once when finalize
  * Other queries: Complete mode not supported as it is infeasible to keep all un-aggregated data in the Result Table.
  *
  * Sinks:
  * File sink: append mode only. exactly-once
  * Kafka sink
  * Foreach sink
  * Console sink (debugging)
  * Memory sink (debugging) This should be used on low data volumes as the entire output is collected and stored in
  * the driver’s memory. Its aim is to allow users to test streaming applications in the Spark shell or other local tests.
  *
  * val outStream = logs.writeStream.format("memory").queryName("logs").start()
  *
  * sql("select * from logs").show(truncate = false)
  *
  *  nc -lk 9999
  *
  * @author sling/ threecuptea
  */
object WordCountWindowed {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: WordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(-1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length > 3) args(3).toInt else windowSize
    if (slideSize > windowSize) {
      System.err.println("slideWindow size can not be greater than" +
        " window size")
      System.exit(-1)
    }

    val windowDuration = s"${windowSize} seconds"
    val slideDuration = s"${slideSize} seconds"

    val spark = SparkSession.builder().appName("WordCountWindow").config("spark.sql.shuffle.partitions", 1).getOrCreate()
    import spark.implicits._

    val rawDF = spark.readStream.format("socket").option("host", host).option("port", port).option("includeTimestamp", true).load()
    val wordsDF = rawDF.as[(String, Timestamp)].flatMap{ case (line, ts) =>
      line.split(" ").map(word => (word, ts))}.toDF("word", "timestamp")

    val wordWindowDF = wordsDF.groupBy(window('timestamp, windowDuration, slideDuration), 'word).count().sort("window")

    val query: StreamingQuery = wordWindowDF.writeStream.format("console").option("truncate", false).option("numRows", 25)
      .outputMode("complete").start()
    query.awaitTermination()

    /*
    val windowDuration = s"${windowSize} seconds"
    val slideDuration = s"${slideSize} seconds"

    val spark = SparkSession.builder().appName("WordCountWindow").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    val lines = spark.readStream.format("socket").
      option("host", host).option("port", port).option("includeTimestamp", true).
      load()

    val words = lines.as[(String, Timestamp)].flatMap{ case (line, ts) =>
     line.split(" ").map(word => (word, ts))}.toDF("word", "timestamp")

    val windowCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().sort("window")

    val query = windowCounts.writeStream.format("console").
      option("truncate", "false").
      option("numRows", 25).outputMode("complete").start()

    query.awaitTermination()

    */

  }

}
