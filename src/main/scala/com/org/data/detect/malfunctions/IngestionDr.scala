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

import com.org.data.detect.malfunctions.Utils.DMFConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

object IngestionDr {
  System.setProperty("hadoop.home.dir", "C:\\SparkWinSetup\\hadoop");
  private val LOGGER = Logger.getLogger(this.getClass.getName)

  /*private val prop = new Properties()
  prop.load(new FileInputStream("config.properties")) // Read config from property file
  private val propsMap=prop.asScala*/

  def main(args: Array[String]): Unit = {

    val LOC_PATH = "C:\\personal\\malfunctions\\loc-hmpaal-data.csv"
    val SERVICE_PATH = "C:\\personal\\malfunctions\\service-data.csv"


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

      locDf.show(5,false)

      /*  read service data  */

      val snDf = spark.read
        .option("delimiter", ",")
        .option("header", "true")
        .csv(SERVICE_PATH);

      snDf.show(5,false)

      /*process the data*/





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
