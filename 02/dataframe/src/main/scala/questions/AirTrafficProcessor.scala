package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** AirTrafficProcessor provides functionalites to
* process air traffic data
* Spark SQL tables available from start:
*   - 'carriers' airliner information
*   - 'airports' airport information
*
* After implementing the first method 'loadData'
* table 'airtraffic',which provides flight information,
* is also available. You can find the raw data from /resources folder.
* Useful links:
* http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
* http://spark.apache.org/docs/latest/sql-programming-guide.html
* https://github.com/databricks/spark-csv
*
* We are using data from http://stat-computing.org/dataexpo/2009/the-data.html
*
* @param spark reference to SparkSession
* @param filePath path to the air traffic data
* @param airportsPath path to the airport data
* @param carriersPath path to the carrier data
*/
class AirTrafficProcessor(spark: SparkSession,
    filePath:String, airportsPath: String, carriersPath: String) {

    import spark.implicits._

    /*
    * Table names for SQL
    * use airTraffic for table name in loadData
    * method.
    */
    val airTraffic = "airtraffic"
    val carriers = "carriers"
    val airports = "airports"

    // load the files and create tables
    // for spark sql
    //DO NOT EDIT
    val carriersTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(carriersPath)
    carriersTable.createOrReplaceTempView(carriers)
    //DO NOT EDIT
    val airportsTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(airportsPath)
    airportsTable.createOrReplaceTempView(airports)

    /** load the data and register it as a table
    * so that we can use it later for spark SQL.
    * File is in csv format, so we are using
    * Spark csv library.
    *
    * Use the variable airTraffic for table name.
    *
    * Example on how the csv library should be used can be found:
    * https://github.com/databricks/spark-csv
    *
    * Note:
    *   If you just load the data using 'inferSchema' -> 'true'
    *   some of the fields which should be Integers are casted to
    *   Strings. That happens, because NULL values are represented
    *   as 'NA' strings in this data.
    *
    *   E.g:
    *   2008,7,2,3,733,735,858,852,DL,1551,N957DL,85,77,42,6,-2,CAE,
    *   ATL,191,15,28,0,,0,NA,NA,NA,NA,NA
    *
    *   Therefore you should remove 'NA' strings and replace them with
    *   NULL. Option 'nullValue' in csv library is useful. However you
    *   don't need to take empty Strings into account.
    *   For instance TailNum field contains empty strings.
    *
    * Correct Schema:
    *   |-- Year: integer (nullable = true)
    *   |-- Month: integer (nullable = true)
    *   |-- DayofMonth: integer (nullable = true)
    *   |-- DayOfWeek: integer (nullable = true)
    *   |-- DepTime: integer (nullable = true)
    *   |-- CRSDepTime: integer (nullable = true)
    *   |-- ArrTime: integer (nullable = true)
    *   |-- CRSArrTime: integer (nullable = true)
    *   |-- UniqueCarrier: string (nullable = true)
    *   |-- FlightNum: integer (nullable = true)
    *   |-- TailNum: string (nullable = true)
    *   |-- ActualElapsedTime: integer (nullable = true)
    *   |-- CRSElapsedTime: integer (nullable = true)
    *   |-- AirTime: integer (nullable = true)
    *   |-- ArrDelay: integer (nullable = true)
    *   |-- DepDelay: integer (nullable = true)
    *   |-- Origin: string (nullable = true)
    *   |-- Dest: string (nullable = true)
    *   |-- Distance: integer (nullable = true)
    *   |-- TaxiIn: integer (nullable = true)
    *   |-- TaxiOut: integer (nullable = true)
    *   |-- Cancelled: integer (nullable = true)
    *   |-- CancellationCode: string (nullable = true)
    *   |-- Diverted: integer (nullable = true)
    *   |-- CarrierDelay: integer (nullable = true)
    *   |-- WeatherDelay: integer (nullable = true)
    *   |-- NASDelay: integer (nullable = true)
    *   |-- SecurityDelay: integer (nullable = true)
    *   |-- LateAircraftDelay: integer (nullable = true)
    *
    * @param path absolute path to the csv file.
    * @return created DataFrame with correct column types.
    */
    def loadDataAndRegister(path: String): DataFrame = {
        val trafficDf = this.spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("nullValue", "NA").load(path)
        trafficDf.show()
        trafficDf.createOrReplaceTempView("airtraffic")
        trafficDf
     }

    //USE can use SPARK SQL or DataFrame transformations

    /** Gets the number of flights for each
    * airplane. 'TailNum' column is unique for each
    * airplane so it should be used. DataFrame should
    * also be sorted by count in descending order.
    *
    * Result looks like:
    *   +-------+-----+
    *   |TailNum|count|
    *   +-------+-----+
    *   | N635SW| 2305|
    *   | N11150| 1342|
    *   | N572UA| 1176|
    *   | N121UA|    8|
    *   +-------+-----+
    *
    * @param df Air traffic data
    * @return DataFrame containing number of flights per
    * TailNum. DataFrame is sorted by count. Column names
    * are TailNum and count
    */
    def flightCount(df: DataFrame): DataFrame = {
        val result = df.select($"TailNum").groupBy("TailNum").agg(count(lit(1)).as("count")).sort(desc("count"))
        result.show()
        result
    }


    /** Which flights were cancelled due to
    * security reasons the most?
    *
    * Example output:
    * +---------+----+
    * |FlightNum|Dest|
    * +---------+----+
    * |     4285| DHN|
    * |     4790| ATL|
    * |     3631| LEX|
    * |     3632| DFW|
    * +---------+----+
    *
    * @return Returns a DataFrame containing flights which were
    * cancelled due to security reasons (CancellationCode = 'D').
    * Columns FlightNum and Dest are included.
    */
    def cancelledDueToSecurity(df: DataFrame): DataFrame = {
        val result = df.select($"FlightNum", $"Dest").where("CancellationCode = 'D'")
        result.show()
        result
    }

    /** What was the longest weather delay between January
    * and march (1.1-31.3)?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |1148|
    * +----+
    *
    * @return DataFrame containing the highest delay which
    * was due to weather.
    */
    def longestWeatherDelay(df: DataFrame): DataFrame = {
        val result = df.filter(row => {
            val month = row.getAs[Int]("Month")
            val dayOfMonth = row.getAs[Int]("DayofMonth")

            (month == 1 && new Range(1, 31, 1).contains(dayOfMonth)) ||
            (month == 2 && new Range(1, 29, 1).contains(dayOfMonth)) ||
            (month == 3 && new Range(1, 31, 1).contains(dayOfMonth))
        }).toDF().agg(max($"WeatherDelay").as("_c0"))
        result.show()
        result
    }

    /** Which airliners didn't fly.
    * Table 'carriers' has this information.
    *
    * Example output:
    * +--------------------+
    * |         Description|
    * +--------------------+
    * |      Air Comet S.A.|
    * |   Aerodynamics Inc.|
    * |  Combs Airways Inc.|
    * |   Evanston Aviation|
    * |Lufthansa Cargo A...|
    * +--------------------+
    *
    * @return airliner descriptions
    */
    def didNotFly(df: DataFrame): DataFrame = {
        val result = df.select($"UniqueCarrier").where("Cancelled = 1").join(this.carriersTable.withColumnRenamed("Code", "UniqueCarrier"), Seq("UniqueCarrier")).select($"Description").distinct().toDF()
        result.show()
        result
    }

    /** Find the airliners which travel
    * from Vegas to JFK. Sort them in descending
    * order by number of flights and append the
    * DataFrame with the data from carriers.csv.
    * Spark SQL table 'carriers' contains this data.
    * Vegas iasa code: LAS
    * JFK iasa code: JFK
    *
    *   Output should look like:
    *
    *   +--------------------+----+
    *   |         Description| Num|
    *   +--------------------+----+
    *   |     JetBlue Airways|1824|
    *   |Delta Air Lines Inc.|1343|
    *   |US Airways Inc. (...| 948|
    *   |American Airlines...| 366|
    *   +--------------------+----+
    *
    * @return DataFrame containing Columns Description
    * (airliner name) and Num(number of flights) sorted
    * in descending order.
    */
    def flightsFromVegasToJFK(df: DataFrame): DataFrame = {
        val projectedFlightsFromVegasToJFK = df.select($"UniqueCarrier").where("Origin = 'LAS' AND Dest = 'JFK'").toDF()
        projectedFlightsFromVegasToJFK.show()

        val joinedFlightInfo = projectedFlightsFromVegasToJFK.join(this.carriersTable.withColumnRenamed("Code", "UniqueCarrier"), "UniqueCarrier").select($"Description").groupBy($"Description").count().sort(desc("count")).withColumnRenamed("count", "Num")
        joinedFlightInfo.show()
        joinedFlightInfo
    }

    /** How much time airplanes spend on moving from
    * gate to the runway and vise versa at the airport on average.
    * This method should return a DataFrame containing this
    * information per airport.
    * Columns 'TaxiIn' and 'TaxiOut' tells time spend on taxiing.
    * Order by time spend on taxiing in ascending order. 'TaxiIn'
    * means time spend on taxiing in departure('Origin') airport
    * and 'TaxiOut' spend on taxiing in arrival('Dest') airport.
    * Column name's should be 'airport' for ISA airport code and
    * 'taxi' for average time spend taxiing
    *
    *
    * DataFrame should look like:
    *   +-------+-----------------+
    *   |airport|             taxi|
    *   +-------+-----------------+
    *   |    BRW|5.084736087380412|
    *   |    OME|5.961294471783976|
    *   |    OTZ|6.866595496262391|
    *   |    DAL|6.983973195822733|
    *   |    HRL|7.019248180512919|
    *   |    SCC|7.054629009320311|
    *   +-------+-----------------+
    *
    * @return DataFrame containing time spend on taxiing per
    * airport ordered in ascending order.
    */
    def timeSpentTaxiing(df: DataFrame): DataFrame = {
        val projectedDF = df.select($"Origin", $"Dest", $"TaxiIn", $"TaxiOut")
        val taxiInMean = projectedDF.groupBy($"Origin").mean("TaxiIn")
        val taxiOutMean = projectedDF.groupBy($"Dest").mean("TaxiOut")
        val taxiMean = taxiInMean.join(taxiOutMean, $"Origin" === $"Dest", "outer").toDF()

        val result = taxiMean.map(row => {

            val avg = {
                if (row.isNullAt(1)) row.getDouble(3)
                else if (row.isNullAt(3)) row.getDouble(1)
                else (row.getDouble(1) + row.getDouble(3)) / 2
            }

            val airportCode = {
                if (row.isNullAt(0)) row.getString(2)
                else row.getString(0)
            }

            (airportCode, avg)
        }).toDF().withColumnRenamed("_1", "Airport").withColumnRenamed("_2", "taxi")

        result.show()
        result
    }

    /** What is the median travel distance?
    * Field Distance contains this information.
    *
    * Example output:
    * +-----+
    * |  _c0|
    * +-----+
    * |581.0|
    * +-----+
    *
    * @return DataFrame containing the median value
    */
    def distanceMedian(df: DataFrame): DataFrame = {
        val medianValue = df.select($"Distance").stat.approxQuantile("Distance", Array(0.5), 0)(0)

        val result = df.withColumn("_c0", lit(medianValue)).select($"_c0").limit(1).toDF()
        result.show()

        result
    }

    /** What is the carrier delay, below which 95%
    * of the observations may be found?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |77.0|
    * +----+
    *
    * @return DataFrame containing the carrier delay
    */
    def score95(df: DataFrame): DataFrame = {
        val percentile = df.select($"CarrierDelay").stat.approxQuantile("CarrierDelay", Array(0.95), 0)(0)

        val result = df.withColumn("_c0", lit(percentile)).select($"_c0").limit(1).toDF()
        result.show()

        result
    }


    /** From which airport are flights cancelled the most?
    * What percentage of flights are cancelled from a specific
    * airport?
    * cancelledFlights combines flight data with
    * location data. Returns a DataFrame containing
    * columns 'airport' and 'city' from 'airports'
    * and the sum of number of cancelled flights divided
    * by the number of all flights (we get percentage)
    * from 'airtraffic' table. Name should be 'percentage'.
    * Lastly result should be ordered by 'percentage' and
    * secondly by 'airport' both in descending order.
    *
    *
    *
    *
    *   Output should look something like this:
    *   +--------------------+--------------------+--------------------+
    *   |             airport|                city|          percentage|
    *   +--------------------+--------------------+--------------------+
    *   |  Telluride Regional|           Telluride|   0.211340206185567|
    *   |  Waterloo Municipal|            Waterloo| 0.17027863777089783|
    *   |Houghton County M...|             Hancock| 0.12264150943396226|
    *   |                Adak|                Adak| 0.09803921568627451|
    *   |Aspen-Pitkin Co/S...|               Aspen| 0.09157716223855286|
    *   |      Sioux Gateway |          Sioux City| 0.09016393442622951|
    *   +--------------------+--------------------+--------------------+
    *
    * @return DataFrame containing columns airport, city and percentage
    */
    def cancelledFlights(df: DataFrame): DataFrame = {
        val filteredDF = df.select($"Origin", $"Cancelled").withColumnRenamed("Origin", "iata")
        val filteredAirportTable = this.airportsTable.select($"iata", $"airport", $"city")
        val joinedTables = filteredAirportTable.join(filteredDF, Seq("iata"))

        val totalFlightsPerAirport = joinedTables.groupBy($"iata").agg(count($"Cancelled"))
        val cancelledFlightsPerAirport = joinedTables.groupBy($"iata").agg(sum($"Cancelled"))
        val averageCancelledFlightsPerAirport = totalFlightsPerAirport.join(cancelledFlightsPerAirport, Seq("iata")).map(row => {
            val totalFlights = row.getLong(1)
            val cancelledFlights = row.getLong(2)

            (row.getString(0), cancelledFlights.toDouble / totalFlights.toDouble)
        }).toDF().withColumnRenamed("_1", "iata").withColumnRenamed("_2", "percentage")

        val result = filteredAirportTable.join(averageCancelledFlightsPerAirport, Seq("iata")).select($"airport", $"city", $"percentage").orderBy(desc("percentage"), desc("airport"))

        result.show()

        result
    }

    /**
    * Calculates the linear least squares approximation for relationship
    * between DepDelay and WeatherDelay.
    *  - First filter out entries where DepDelay < 0
    *  - there are definitely multiple data points for a single
    *    DepDelay value so calculate the average WeatherDelay per
    *    DepDelay
    *  - Calculate the linear least squares (c+bx=y) where
    *    x equals DepDelay and y WeatherDelay. c is the constant term
    *    and b is the slope.
    *
    *  
    * @return tuple, which has the constant term first and the slope second
    */
    def leastSquares(df: DataFrame):(Double, Double) = {
        val filteredDF = df.select($"DepDelay", $"WeatherDelay").where($"DepDelay" >= 0)
        val averagesRow = filteredDF.agg(avg($"DepDelay"), avg($"WeatherDelay")).first()

        val averages = (averagesRow.getDouble(0), averagesRow.getDouble(1))

        filteredDF.show()
        println(averages)

        averages
    }

    /**
    * Calculates the running average for DepDelay per day.
    * Average should be taken from both sides of the day we
    * are calculating the running average. You should take
    * 5 days from both sides of the day you are counting the running
    * average into consideration. For instance
    * if we want to calculate the average for the 10th of
    * January we have to take days: 5,6,7,8,9,10,11,12,13,14 and
    * 15 into account.
    *
    * Complete example
    * Let's assume that data looks like this
    *
    * +----+-----+---+--------+
    * |Year|Month|Day|DepDelay|
    * +----+-----+---+--------+
    * |2008|    3| 27|      12|
    * |2008|    3| 27|      -2|
    * |2008|    3| 28|       3|
    * |2008|    3| 29|      -5|
    * |2008|    3| 29|      12|
    * |2008|    3| 30|       5|
    * |2008|    3| 31|      47|
    * |2008|    4|  1|      45|
    * |2008|    4|  1|       2|
    * |2008|    4|  2|      -6|
    * |2008|    4|  3|       0|
    * |2008|    4|  3|       4|
    * |2008|    4|  3|      -2|
    * |2008|    4|  4|       2|
    * |2008|    4|  5|      27|
    * +----+-----+---+--------+
    *
    *
    * When running average is calculated
    * +----------+------------------+
    * |      date|    moving_average|
    * +----------+------------------+
    * |2008-03-27|13.222222222222221|
    * |2008-03-28|              11.3|
    * |2008-03-29| 8.846153846153847|
    * |2008-03-30| 8.357142857142858|
    * |2008-03-31|               9.6|
    * |2008-04-01|               9.6|
    * |2008-04-02|10.307692307692308|
    * |2008-04-03|10.916666666666666|
    * |2008-04-04|              12.4|
    * |2008-04-05|13.222222222222221|
    * +----------+------------------+
    *
    * For instance running_average for 
    * date 2008-04-03 => (-5+12+5+47+45+2-6+0+4-2+2+27)/12
    * =10.916666
    *
    *
    * @return DataFrame with schema 'date' (YYYY-MM-DD) (string type) and 'moving_average' (double type)
    * ordered by 'date'
    */
    def runningAverage(df: DataFrame): DataFrame = {
        ???
    }
}
/**
*
*  Change the student id
*/
object AirTrafficProcessor {
    val studentId = "XXXXXX"
}
