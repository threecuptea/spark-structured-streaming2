package org.freemind.spark.streaming

import java.sql._

import org.apache.spark.sql.{Row, ForeachWriter}

//We need to create this table first
class JDBCSink(url: String, user: String, pwd: String, table: String) extends ForeachWriter[(String, String, String, String)] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  override def process(v: (String, String, String, String)): Unit = {
        //YOU MUST HAVE '', otherwise errorlike the right syntax to use near '22:39:31.168,7,2018-05-12 22:39:39.209)' at line 1
       statement.executeUpdate("INSERT INTO " + table + " (impressionAdId, impressionTime, clickAdId, clickTime) VALUES ('" +
         v._1 +  "','" + v._2 + "','" + v._3 + "','" + v._4 + "')")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()

  }
}
