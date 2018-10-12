package questions

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, PartitionID, PartitionStrategy, VertexId}

import scala.util.matching.Regex


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
    val file = this.spark.sparkContext.textFile(filePath)

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
        data
          .map { a => a.split("\t") }
          .map { a => Array(a{1}, a{8}, a{16}) }
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {
        data
          .filter { a => a{1}.equals(countryCode) }
          .map { a => a{2}.toInt }
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
        data
          .map { a => a{0} }
          .flatMap { a => a.split(" ") }
          .groupBy { a => a }
          .map { a => (a._1, a._2.size) }
          .sortBy { a => a._2 }
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
      val mostFrequentCountryCode = data
        .map { a => a {1}}
        .groupBy { b => b }
        .sortBy({ c => c._2.size }, false).first()._1

      val mostFrequentCountryNameEntry = this.spark.read.format("csv").option("header", "true").load(path)
        .filter { r => r.getString(1).equals(mostFrequentCountryCode) }.select("Name")

      if (mostFrequentCountryNameEntry.count() > 0) mostFrequentCountryNameEntry.first().getString(0) else ""
    }

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

        this.file
          .map { a => a.split("\t") }
          .filter { a => a{1}.split(" ")
            .exists(_.equalsIgnoreCase("hotel")) }
          .map { a => (a{4}.toDouble, a{5}.toDouble) }
          .filter { d => haversineDistance(lat, long, d._1, d._2) <= 10000 }.count().intValue()
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

        val regexString = "^\\s*([1-9][0-9]*)\\s*\\|\\s*([1-9][0-9]*)\\s*$"
        val compiledRegex = new Regex(regexString, "firstVertex", "secondVertex")

        val graphRDD = this.spark.sparkContext.textFile(path)

        val rawEdges = graphRDD
          .filter(_.matches(regexString))
          .map({ validEdgeInfo =>
            val m = compiledRegex.findFirstMatchIn(validEdgeInfo).get
            (m.group("firstVertex").toLong, m.group("secondVertex").toLong)
          })

        val graph = Graph.fromEdgeTuples(rawEdges, 1, Some(PartitionStrategy.RandomVertexCut)).mapVertices((id, _) => id.toInt)

        graph
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
        graph.outDegrees
          .reduce({ (a, b) => if (a._2 > b._2) a else b })._1.toInt
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int,Int]): Int = {
      graph.pageRank(0.0001).vertices
        .join(graph.vertices)
        .sortBy(_._2._1, ascending = false).first()._1.toInt
    } 
}

/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "727134"
}