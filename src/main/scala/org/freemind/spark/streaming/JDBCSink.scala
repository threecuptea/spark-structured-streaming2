package org.freemind.spark.streaming

import java.sql._

import org.apache.spark.sql.{Row, ForeachWriter}

//We need to create this table first
class JDBCSink(url: String, user: String, pwd: String, table: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  override def process(r: Row): Unit = {
     //RESORT to raw SQL.  WE MUST HAVE '', otherwise errorlike the right syntax to use near
     //'22:39:31.168,7,2018-05-12 22:39:39.209)' at line 1
    //The generic approach won't work if it is null, 'Incorrect integer value: 'null' for column 'clickAdId' at row 1'
       statement.executeUpdate("INSERT INTO " + table + " VALUES (" +
       r.toSeq.map(x => Option(x)).map(o => if (o.isDefined) "'" + o.get + "'" else null).mkString(", ") +
         ")")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()

  }
}
