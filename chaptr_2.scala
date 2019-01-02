// settings
// set parttions to 5 to reduce exeute time on a local instance
spark.conf.set("spark.sql.shuffle.parititions", "5")

// load the flight data for 2015
val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("develop/def_guide/data/flight-data/csv/2015-summary.csv")

// sorting by count
flightData2015.sort("count").take(2)

// Dataframes and SQL

// Setup temp view
flightData2015.createOrReplaceTempView("flight_data_2015")

val sqlWay = spark.sql("""
  SELECT DEST_COUNTRY_NAME, count(1)
  FROM flight_data_2015
  GROUP BY DEST_COUNTRY_NAME
  """)
