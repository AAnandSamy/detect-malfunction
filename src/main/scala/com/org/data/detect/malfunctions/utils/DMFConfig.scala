package com.org.data.detect.malfunctions.utils

import scala.collection.JavaConverters._
import scala.collection.mutable

object DMFConfig {


  def orientWrOpts(props: mutable.Map[String, String]): Map[String, String] = {
    val params = Map[String, String](
      "dburl" ->"db",
      "user" -> "user",
      "password" -> "password",
      "class" -> "test_table"
    )
    params

  }


  def orientRdOpts(props: mutable.Map[String, String]): Map[String, String] = {
    val params = Map[String, String](
      "dburl" -> "db",
      "user" -> "user",
      "password" -> "password",
      "class" -> "test_table",
      "query" -> s"select * from test_table where teststring = 'asdf'"
    )
    params

  }

}
