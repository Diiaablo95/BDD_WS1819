package questions

import org.apache.spark.sql.SparkSession
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

      val filtered = Array(
        Array("Courtyard Long Beach Downtown","US","19"),
        Array("Comfort Inn & Suites Near Universal Studios","US","208"),
        Array("Best Western Meridian Inn & Suites. Anaheim-Orange","US","42"),
        Array("Eurostars Dylan","US","39"),
        Array("Best Western San Mateo Los Prados Inn","US","1"),
        Array("Americas Best Value Inn Extended Stay Civic Center Ex.Abigail","US","25"),
        Array("Holiday Inn Express Hotel & Suites San Francisco Fishermans Wharf","US","7"),
        Array("Aida Shared Bathroom","US","17"),
        Array("Citigarden Formerly Ramada Airport North","US","6"),
        Array("Holiday Inn Express & Suites Sfo North","US","5"),
        Array("Holiday Inn In'Tl Airport","US","7"),
        Array("Galaxy Motel","US","6"),
        Array("Ymca Greenpoint Shared Bathroom","US","8"),
        Array("Ymca Flushing Shared Bathroom","US","21"),
        Array("Seton","US","13"),
        Array("The Roosevelt","US","29"),
        Array("The Village At Squaw Valley","US","1896"),
        Array("Ritz Carlton Highlands Lake Tahoe","US","1967"),
        Array("The Tuscan A Kimpton Hotel","US","8"),
        Array("Comfort Inn Watsonville","US","37"),
        Array("Days Inn & Suites Santa Barbara","US","43"),
        Array("Best Western Plus Pepper Tree","US","61"),
        Array("Hyatt","US","9"),
        Array("Comfort Inn By The Bay - San F","US","41"),
        Array("Comfort Inn & Suites Airport","US","5"),
        Array("Indigo Santa Barbara","US","6"),
        Array("Canary Hotel","US","25"),
        Array("Motel 6 Ventura Beach 218","US","6"),
        Array("Comfort Inn Ventura Beach","US","5"),
        Array("Crowne Plaza Ventura","US","14"),
        Array("Four Seasons The Biltmore Santa Barbara","US","26"),
        Array("Inn By The Harbor - Santa Barb","US","11"),
        Array("Fess Parker'S Doubletree - San","US","3"),
        Array("Best Western Encinita Lodge","US","50"),
        Array("Best Western Plus Pepper Tree Inn","US","61"),
        Array("Canary A Kimpton Hotel","US","22"),
        Array("Hotel Goleta - Santa Barbara","US","17"),
        Array("Holiday Inn Express Santa Barbara","US","11"),
        Array("Dolphin Bay Resort & Spa","US","24"),
        Array("Best Western Plus Sonora Oaks","US","653"),
        Array("Comfort Inn & Suites Sfo Airport","US","6"),
        Array("Hilton Garden Inn San Francisco Arpt North","US","12"),
        Array("Embassy Suites San Fran","US","4"),
        Array("Best Western Plus Grosvenor Ai","US","4"),
        Array("Comfort Inn & Suites San Franc","US","6"),
        Array("Days Inn San Francisco South Oyster Point Airport","US","12"),
        Array("Baymont Inn & Suites Miami Airport West","US","7"),
        Array("Best Western Premier Miami International Airport","US","11"),
        Array("Residence Inn Miami Airport South","US","10"),
        Array("Ramada Inn Miami Airport North","US","9"))
      val data = spark.sparkContext.parallelize(filtered)

        processor.loadSocial(this.getClass().getResource("/graph_test.dat").toString())

        //stop spark
        spark.stop()
    }
}
