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
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

object IngestionDr {

  private val LOGGER = Logger.getLogger(this.getClass.getName)
  private val prop = new Properties()
  prop.load(new FileInputStream("src/main/resources/app-config.properties")) // Read config from property file
  private val propsMap=prop.asScala
  System.setProperty("hadoop.home.dir", propsMap("hadoop.home.dir"));

  def main(args: Array[String]): Unit = {

      val LOC_PATH = propsMap("hdfs.in.loc")+"\\hmpaal-data*"
      val SERVICE_PATH = propsMap("hdfs.in.loc")+"\\service-data*"


    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("detect-malfunctions")
      .getOrCreate()

    import spark.implicits._

    try {

      /*  read location data  */

      val locDf = spark.read
        .option("delimiter", ",")
        .option("header", "true")
        .csv(LOC_PATH);

      /*  read service data  */

      val snDf = spark.read
        .option("delimiter", ",")
        .option("header", "true")
        .csv(SERVICE_PATH);

      /*process the data*/
      //join key

      locDf.join(snDf,"join_key")
        .show(20,false)






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
