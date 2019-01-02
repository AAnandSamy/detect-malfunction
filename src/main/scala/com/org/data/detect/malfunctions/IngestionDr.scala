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
import org.apache.spark.orientdb._

import scala.collection.JavaConverters._

object IngestionDr {
  System.setProperty("hadoop.home.dir", "C:\\software\\hadoop");
  private val LOGGER = Logger.getLogger(this.getClass.getName)

  private val prop = new Properties()
  prop.load(new FileInputStream("config.properties")) // Read config from property file
  private val propsMap=prop.asScala

  def main(args: Array[String]): Unit = {

    val FILE_IN_PATH = "C:\\Aannd\\malfunction\\hmpaal data.csv"
    val wrOpts=DMFConfig.orientWrOpts(propsMap)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("detect-malfunctions")
      .getOrCreate()

    import spark.implicits._

    try {

      /*  read source file */

      val srcDf = spark.read
        .option("delimiter", ",")
        .option("header", "true")
        .csv(FILE_IN_PATH);

      srcDf.show(5,false)

      /*process the data*/

      val finalDf = srcDf

      /*data persist into Orient*/

      finalDf
        .write
        .format("org.apache.spark.orientdb.documents")
        .options(wrOpts)
        .mode(SaveMode.Overwrite)
        .save()


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
