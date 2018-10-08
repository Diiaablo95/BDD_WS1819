package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
    def main(args: Array[String]) = {
	
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val spark = SparkSession.builder
          .master("local")
          .appName("main")
          .config("spark.driver.memory", "5g")
          .getOrCreate()

        val path = getClass().getResource("/sample.txt").toString
        val processor = new GeoProcessor(spark, path)

        processor.hotelsInArea(37.80626, -122.41628)

        //stop spark
        spark.stop()
    }
}
