package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import scala.math._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import questions.Main.getClass

import scala.io.Source


/** GeoProcessor provides functionalites to 
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
*
* @param spark reference to SparkSession 
* @param filePath path to file that should be modified
*/
class GeoProcessor(spark: SparkSession, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = spark.sparkContext.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character. 
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */
        data.map { rddLine => rddLine.split("\t") }.map { splittedLine => Array(splittedLine{1}, splittedLine{8}, splittedLine{16}) }
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {
        data.filter { a => a{1}.equals(countryCode) }.map { b => b{2}.toInt }
    }



    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {
        data.sum() / data.count()
    }

    /** mostCommonWords calculates what is the most common 
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {
        print(data)
        val names = data.map { a => a{0} }
        val words = names.flatMap { a => a.split(" ") }
        val groupedWords = words.groupBy { a => a }
        val wordsCount = groupedWords.map { a => (a._1, a._2.size) }
        val wordsByFrequency = wordsCount.sortBy { a => a._2 }
        wordsByFrequency
    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
        val mostFrequentCountryCode = data.map { a => a{1} }.groupBy { b => b }.sortBy { c => c._2.size }.first()._1

        val csvFile = this.spark.read.format("csv").option("header", "true").load(path)

        val mostFrequentCountryNameEntry = csvFile.filter { r => r.getString(1).equals(mostFrequentCountryCode) }.select("Name").first()

        if (mostFrequentCountryNameEntry == null) {
          ""
        } else {
          mostFrequentCountryNameEntry.getString(0)
        }
    }

//
    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {

        val haversineDistance = (lat1:Double, lon1:Double, lat2:Double, lon2:Double) => {
            val dLat = (lat2 - lat1).toRadians
            val dLon= (lon2 - lon1).toRadians
            val EARTH_RADIUS = 6378137d

            val a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
            val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            EARTH_RADIUS * c
        }

        val a = this.file.map { a => a.split("\t") }
        var asd = a.count()
        val b = a.filter { b => b{1}.split(" ").exists(_.equalsIgnoreCase("hotel")) }
        asd = b.count()
        val c = b.map { c => (c{4}.toDouble, c{5}.toDouble) }
        asd = c.count()
        val d = c.filter { d =>
          val distance = haversineDistance(lat, long, d._1, d._2)
          distance <= 10000
        }
        asd = d.count()
        val e = d.count().intValue()

//        this.file.map { a => a.split("\t") }.map { b => (b{4}.toDouble, b{5}.toDouble) }.filter { c => distance(lat, long, c._1, c._2) <= 10000d }.count()intValue()
        e
    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int,Int] = {
        ???
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
        ???
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int,Int]): Int = {
        ???
    } 
}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "727134"
}

//object Haversine {
//
//  val EARTH_RADIUS = 6378137d
//
//  /**
//    * Calculates distance basing on [[EARTH_RADIUS]]. The result is in meters.
//    *
//    * @param startLon - a start lon
//    * @param startLat - a start lan
//    * @param endLon   - an end lon
//    * @param endLat   - an end lat
//    */
//  def apply(startLon: Double, startLat: Double, endLon: Double, endLat: Double): Double =
//    apply(startLon, startLat, endLon, endLat, EARTH_RADIUS)
//
//  /**
//    * Calculates distance, in R units
//    *
//    * @param startLon - a start lon
//    * @param startLat - a start lan
//    * @param endLon   - an end lon
//    * @param endLat   - an end lat
//    * @param R        - Earth radius, can be in any unit, in fact this value
//    *                   defines a physical meaning of this function
//    */
//  def apply(startLon: Double, startLat: Double, endLon: Double, endLat: Double, R: Double): Double = {
//    val dLat = math.toRadians(endLat - startLat)
//    val dLon = math.toRadians(endLon - startLon)
//    val lat1 = math.toRadians(startLat)
//    val lat2 = math.toRadians(endLat)
//
//    val a =
//      math.sin(dLat / 2) * math.sin(dLat / 2) +
//        math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
//    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
//
//    R * c
//  }
//}