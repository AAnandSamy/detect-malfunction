/*
# Program      : IngestionDr.scala
# Created      : 12/18/2018
# Description  : This class is used to ingest the data into HDFS dir and redis
# Parameters   :
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 12/18/2018   Anand Ayyasamy               Creation
# ===========  ===================  ============================================
*/
package com.org.data.detect.malfunctions

import java.io.FileInputStream
import java.util.Properties

import com.sun.deploy.association.utility.AppConstants
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

object IngestionDr {
  private val LOGGER = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("detect-malfunctions")
      .getOrCreate()

    import spark.implicits._

    try {



    }

    catch {
      case exception: Exception =>
        LOGGER.error(exception.printStackTrace())
        sys.exit(1)

    }
    finally {
      spark.close()
      LOGGER.info("Closing the SparkSession")
      sys.exit(0)
    }


  }


}
